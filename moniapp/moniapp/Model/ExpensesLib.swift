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

    public func save() {
        do {
            try lib.save()
        } catch {
            print("Error saving: \(error)")
        }
    }
}

extension MoniErrorType: @retroactive CustomStringConvertible {
    public var description: String {
        switch self {
        case .lib(let cause):
            return "MoniLib error: \(cause)"
        case .domain(let error):
            return "Domain error: \(error)"
        }
    }
}

extension LibErrorCause: @retroactive CustomStringConvertible {
    public var description: String {
        switch self {
        case .sender:
            return "internal communication failed"
        case .path:
            return "invalid storage path"
        case .threading:
            return "threading failure"
        case .stateLoad(let message):
            return "failed to load state: \(message)"
        }
    }
}

extension MoniDomainError: @retroactive CustomStringConvertible {
    public var description: String {
        switch self {
        case .validation(let error):
            return "\(error.field) is invalid (\(error.cause))"
        case .expenseNotFound(let id):
            return "expense not found: \(id)"
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
    }


    public func plainListModel() -> PlainListModel {
        PlainListModel(lib: self.lib)
    }
    
    public func calculateStatistics() {
        if statisticsTask == nil {
            self.statisticsTask = Task { [weak self, lib] in
                for await statistics in lib.statistics() {
                    guard let self else { return }
                    self.latestStatistics = statistics
                    showStatistics = true
                }
            }
        }
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

extension MoniExpense {
    static func random(maxAmount: Int64) -> MoniExpense {
        let comments = [
            "Double Espresso", "Groceries", "Taxi", "Lunch", "J.S.O. DSP Book",
            "Vinyl record", "Gym", "Orange juice", "Autechre latest WARP reedition", "Gift"
        ]
        return MoniExpense(
            date: nil,
            amount: Int64.random(in: 1_00...maxAmount),
            comment: comments.randomElement(),
            category: ExpenseCategory.allCases.randomElement() ?? .essential
        )
    }
}

public enum ExpenseListItem: Identifiable {
    case expense(PlainListItem)
    case fault(UInt64)

    public var id: UInt64 {
        switch self {
            case .expense(let expense):
            return expense.id
        case .fault(let id):
            return id
        }
    }

    init(id: UInt64, expense: PlainListItem?) {
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
    @ObservationIgnored private var cachedItems: [UInt64: PlainListItem] = [:]
    
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
            try lib.addExpense(expense: .random(maxAmount: MoniInfo.getMax() / 10000))
        } catch {
            print("Error adding expense: \(error)")
        }
    }
    
    public func hint(id: UInt64) {
        do {
            try self.listHandler.hint(hint: id)
        } catch {
            print("Error hinting: \(error)")
        }
    }
    
    deinit {
        updatesTask?.cancel()
    }
    
}
