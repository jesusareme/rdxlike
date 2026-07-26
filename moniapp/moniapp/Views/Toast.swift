import SwiftUI
import Monilib

extension View {
    func showToast(errors: Binding<[MoniError]>) -> some View {
        modifier(ToastModifier(errors: errors))
    }
}

private enum ToastPresentationSlot: Equatable {
    case available
    case waiting
    case shown(MoniError)
}

private enum ToastPresentationItem {
    case error(MoniError)
    case info(String)
}

private struct ToastModifier: ViewModifier {
    @Binding var errors: [MoniError]
    @State var currentError: ToastPresentationSlot = .available
    
    func body(content: Content) -> some View {
        content
            .overlay(alignment: .bottom) {
                if case let .shown(error) = self.currentError {
                    HStack {
                        Text(error.errorType.description)
                        Spacer()
                        Button("Close", systemImage: "xmark.circle.fill") {
                            currentError = .waiting
                            Task {
                                try await Task.sleep(for: .seconds(1))
                                if !errors.isEmpty {
                                    currentError = .shown(errors.removeFirst())
                                } else {
                                    currentError = .available
                                }
                            }
                        }
                    }
                    .foregroundColor(.red)
                    .padding()
                    .background(.regularMaterial, in: .capsule)
                    .padding(.bottom, 30)
                    .transition(.move(edge: .bottom).combined(with: .opacity))
                }
            }
            .animation(.spring, value: currentError)
            .onChange(of: errors, initial: true) { _, new in
                if !errors.isEmpty, currentError == .available {
                    currentError = .shown(errors.removeFirst())
                }
            }
    }
}
