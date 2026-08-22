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
                    if case let .fault(id) = item {
                        self.model.hint(id: id)
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
            VStack {
                Text("\(plainListItem.comment ?? "<no comment>")")
                Rectangle().fill(Color.blue).frame(height: 1).padding(.horizontal, 40)
                Text(Double(plainListItem.amount) / 100.0, format: .currency(code: "EUR"))
                Text(plainListItem.date, format: .dateTime)

            }
        case .fault(let uUID):
            Text("Fault for \(uUID)")
        }
    }
}
