import Observation
import Foundation
import Monilib

public enum ExpensesLibRuntime {
    case lib, testing
}

@MainActor @Observable public final class ExpensesLib {
    @ObservationIgnored private let lib: MoniLib
    
    public init(runtime: ExpensesLibRuntime) {
        // TODO: testing env.
        let config = LibConfig(logLevel: .debug, clock: .system)
        do {
            lib = try MoniLib(path: URL.documentsDirectory.path(), config: config)
        } catch {
            fatalError("Unrecoverable error. Failed to initialize MoniLib: \(error)")
        }
    }
    
    public func rootModel() -> ExpensesRootModel {
        ExpensesRootModel(lib: self.lib)
    }
}

extension MoniErrorType: @retroactive CustomStringConvertible {
    public var description: String {
        switch self {
        case .lib(let message):
            return "MoniLib error: \(message)"
        case .domain(let message):
            return "Domain error: \(message)"
        }
    }
}

@MainActor @Observable public final class ExpensesRootModel {
    @ObservationIgnored private let lib: MoniLib
    @ObservationIgnored private var errorsTask: Task<(), Never>?
    @ObservationIgnored private var statisticsTask: Task<(), Never>?
    public var errors: [MoniError] = []
    public var latestStatistics: MoniStatistics?
    public var showStatistics = false
    
    fileprivate init(lib: MoniLib) {
        self.lib = lib
        self.errorsTask = Task { [weak self, lib] in
            for await newErrors in lib.errors() {
                guard let self else { return }
                self.errors.append(contentsOf: newErrors)
            }
        }
        self.statisticsTask = Task { [weak self, lib] in
            for await statistics in lib.statistics() {
                guard let self else { return }
                self.latestStatistics = statistics
                showStatistics = true
            }
        }
    }


    public func plainListModel() -> PlainListModel {
        PlainListModel(lib: self.lib)
    }
    
    public func calculateStatistics() {
        do {
            try lib.calculateStatisticsAll()
        } catch {
            print("Error: could not start calculating statistics: \(error)")
        }
    }
    
    deinit {
        self.errorsTask?.cancel()
        self.statisticsTask?.cancel()
    }
}

public enum ExpenseListItem: Identifiable {
    case expense(PlainListItem)
    case fault(UUID)
    
    public var id: UUID {
        switch self {
            case .expense(let expense):
            return expense.id
        case .fault(let id):
            return id
        }
    }
    
    init(id: UUID, expense: PlainListItem?) {
        if let expense {
            self = .expense(expense)
        } else {
            self = .fault(id)
        }
    }
}

@MainActor @Observable public final class PlainListModel {
    @ObservationIgnored private let lib: MoniLib
    @ObservationIgnored private let listHandler: PlainListViewHandler
    @ObservationIgnored private var updatesTask: Task<Void, Never>?
    @ObservationIgnored private var cachedItems: [UUID: PlainListItem] = [:]
    
    public var list: [ExpenseListItem] = []
    

    fileprivate init(lib: MoniLib) {
        self.lib = lib
        self.listHandler = lib.createPlainListView()
        self.updatesTask = Task { [weak self] in
            guard let listHandler = self?.listHandler else { return }
            for await update in listHandler.subscribe() {
                guard let self else { return }
                self.cachedItems.merge(update.updated.map({ updated in (updated.id, updated) }), uniquingKeysWith: { $1 })
                self.list = update.ids.map({ ($0, self.cachedItems[$0]) }).map(ExpenseListItem.init)
            }
        }
    }
    
    public func add() {
        do {
            let expense = MoniExpense(date: nil, amount: 4200, comment: "mola", category: .essential)
            try lib.addExpense(expense: expense)
        } catch {
            print("Error adding expense: \(error)")
        }
    }
    
    public func hint(uuid: UUID) {
        do {
            try self.listHandler.hint(hint: uuid)
        } catch {
            print("Error hinting: \(error)")
        }
    }
    
    deinit {
        updatesTask?.cancel()
    }
    
}
