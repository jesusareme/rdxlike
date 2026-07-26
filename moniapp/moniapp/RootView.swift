import SwiftUI
import Monilib

struct RootView: View {
    @State private var model: ExpensesRootModel
    
    private let list1: PlainListModel
    private let list2: PlainListModel
    
    init(model: ExpensesRootModel) {
        self.model = model
        list1 = model.plainListModel()
        list2 = model.plainListModel()
    }
    
    var body: some View {
        Button("Calculate statistics") {
            model.calculateStatistics()
        }
        TabView {
            Tab("List1", systemImage: "list.bullet") {
                PlainListView(model: self.list1)
            }
            Tab("List2", systemImage: "list.number") {
                PlainListView(model: self.list2)
            }
        }
        .showToast(errors: $model.errors)
        .alert("Statistics",
               isPresented: $model.showStatistics,
               presenting: model.latestStatistics,
               actions: { _ in
                    Button("OK") {}
                },
               message: { statistics in
                    Text("We have \(statistics.len) expenses")
                }
        )
    }
}
