// swift-tools-version: 6.1
import PackageDescription

let package = Package(
    name: "swift-acp",
    platforms: [
        .macOS(.v12),
        .iOS(.v15),
        .tvOS(.v15),
        .watchOS(.v8)
    ],
    products: [
        .library(name: "ACPModel", targets: ["ACPModel"]),
        .library(name: "ACP", targets: ["ACP"]),
        .library(name: "ACPHTTP", targets: ["ACPHTTP"]),
        .library(name: "ACPRegistry", targets: ["ACPRegistry"])
    ],
    dependencies: [
        .package(url: "https://github.com/mattt/swift-yyjson.git", from: "0.6.0", traits: ["strictStandardJSON"])
    ],
    targets: [
        // Core model types (platform-independent)
        .target(
            name: "ACPModel",
            dependencies: [.product(name: "YYJSON", package: "swift-yyjson")],
            path: "Sources/ACPModel"
        ),
        // Main ACP client/agent runtime
        .target(
            name: "ACP",
            dependencies: ["ACPModel", .product(name: "YYJSON", package: "swift-yyjson")],
            path: "Sources/ACP"
        ),
        // HTTP/WebSocket transport (optional)
        .target(
            name: "ACPHTTP",
            dependencies: ["ACP", "ACPModel", .product(name: "YYJSON", package: "swift-yyjson")],
            path: "Sources/ACPHTTP"
        ),
        // Agent registry (macOS only)
        .target(
            name: "ACPRegistry",
            path: "Sources/ACPRegistry"
        ),
        // Tests
        .testTarget(
            name: "ACPTests",
            dependencies: ["ACP", "ACPModel"]
        ),
        .testTarget(
            name: "ACPModelTests",
            dependencies: ["ACPModel"]
        ),
        .testTarget(
            name: "ACPHTTPTests",
            dependencies: ["ACPHTTP", "ACP", "ACPModel"]
        ),
        .testTarget(
            name: "ACPRegistryTests",
            dependencies: ["ACPRegistry"]
        )
    ]
)
