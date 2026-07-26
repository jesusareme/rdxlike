import SwiftUI
import Monilib

@main
struct MoniApp: App {
    
    private let lib = ExpensesLib(runtime: .lib)
    
    var body: some Scene {
        WindowGroup {
            RootView(model: lib.rootModel())
        }
    }
}
