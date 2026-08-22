import SwiftUI
import UIKit
import Monilib

@main
struct MoniApp: App {
    @Environment(\.scenePhase) private var scenePhase

    private let lib = ExpensesLib(runtime: .lib)

    var body: some Scene {
        WindowGroup {
            RootView(model: lib.rootModel())
                .onReceive(NotificationCenter.default.publisher(for: UIApplication.didReceiveMemoryWarningNotification)) { _ in
                    lib.save()
                }
        }
        .onChange(of: scenePhase) { _, newPhase in
            switch newPhase {
            case .inactive, .background:
                lib.save()
            default:
                break
            }
        }
    }
}