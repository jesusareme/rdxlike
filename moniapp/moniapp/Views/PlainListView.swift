import SwiftUI
import Monilib

struct PlainListView: View {
    @State var model: PlainListModel
    
    init(model: PlainListModel) {
        self.model = model
    }
    
    var body: some View {
        Button("Add expense") {
            self.model.add()
        }
        List(model.list) { item in
            ExpenseRow(item: item)
                .onAppear() {
                    if case let .fault(uuid) = item {
                        self.model.hint(uuid: uuid)
                    }
                }
        }
    }
}

struct ExpenseRow: View {
    let item: ExpenseListItem
    
    var body: some View {
        switch item {
        case .expense(let plainListItem):
            Text("\(plainListItem.id) - \(plainListItem.amount) on date \(plainListItem.date)")
        case .fault(let uUID):
            Text("Fault for \(uUID)")
        }
    }
}
