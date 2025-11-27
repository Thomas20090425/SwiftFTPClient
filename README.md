# SwiftFTPClient Fixed 13.4 Xcode Error

SwiftFTPClient is a modern, Swift-based FTP client library that leverages the `Network` framework for efficient and reliable FTP operations. It provides an easy-to-use interface for connecting to FTP servers, uploading files and data, and managing transfers with progress tracking.

## Requirements

- macOS 10.15.4 or later
- iOS 13.0 or later
- tvOS 13.0 or later
- watchOS 6.0 or later

Note: While the package specifies macOS 10.15 as the minimum version, certain functionalities require macOS 10.15.4 or later. The library will check for this at runtime and fail gracefully if the requirement is not met.

## Features

- Asynchronous API using Swift concurrency
- Support for uploading files and raw data
- Progress tracking for uploads
- Cancellable operations
- Error handling with custom `FTPError` type
- Supports macOS 10.15+, iOS 13+, tvOS 13+, and watchOS 6+

## Installation

### Swift Package Manager

You can add SwiftFTPClient to your project using Swift Package Manager. In Xcode, go to File > Swift Packages > Add Package Dependency and enter the repository URL:

```
https://github.com/fenixkim/SwiftFTPClient.git
```

Alternatively, you can add it to your `Package.swift` file:

```swift
dependencies: [
    .package(url: "https://github.com/fenixkim/SwiftFTPClient.git", from: "1.0.0")
]
```

## Usage

### Initialization

To create an instance of SwiftFTPClient, you need to provide the necessary configuration:

```swift
import SwiftFTPClient

let credentials = FTPCredentials(host: "ftp.example.com", port: 21, username: "user", password: "pass")
let remotePath = "/upload/path"
let ftpClient = FTPClient(credentials: credentials, remotePath: remotePath)
```

#### Constructor Parameters

The `FTPClient` initializer takes the following parameters:

```swift
public init(credentials: FTPCredentials, remotePath: String, progress: Progress? = nil, bufferSize: Int = 512 * 1024)
```

- `credentials`: An `FTPCredentials` struct containing the server connection details.
  - `host`: The hostname or IP address of the FTP server.
  - `port`: The port number of the FTP server (usually 21 for standard FTP).
  - `username`: The username for authentication.
  - `password`: The password for authentication.
- `remotePath`: The directory path on the FTP server where files will be uploaded.
- `progress`: An optional `Progress` object for tracking overall progress across multiple operations or app sessions. If not provided, the client will create its own internal progress tracker.
- `bufferSize`: The size of the buffer used for file transfers, in bytes. Default is 512 KB (524,288 bytes). Adjust this value to optimize performance based on your specific use case and network conditions.

### Uploading Files

SwiftFTPClient provides two methods for uploading files: one using async/await and another using completion handlers.

#### Using async/await

```swift
let filesToUpload: [FTPUploadable] = [
    .file(url: URL(fileURLWithPath: "/path/to/local/file1.txt"), remoteFileName: "file1.txt"),
    .data(data: Data("Sample data".utf8), remoteFileName: "sample.txt")
]

do {
    try await ftpClient.upload(files: filesToUpload) { progress in
        print("Overall progress: \(progress.fractionCompleted * 100)%")
    }
    print("All files uploaded successfully.")
} catch {
    print("An error occurred: \(error)")
}
```

#### Using completion handlers

```swift
let filesToUpload: [FTPUploadable] = [
    .file(url: URL(fileURLWithPath: "/path/to/local/file1.txt"), remoteFileName: "file1.txt"),
    .data(data: Data("Sample data".utf8), remoteFileName: "sample.txt")
]

ftpClient.upload(files: filesToUpload, progressHandler: { progress in
    print("Overall progress: \(progress.fractionCompleted * 100)%")
}, completionHandler: { result in
    switch result {
    case .success:
        print("All files uploaded successfully.")
    case .failure(let error):
        print("An error occurred: \(error)")
    }
})
```

### Cancelling Uploads

You can cancel ongoing upload operations at any time:

```swift
ftpClient.cancel()
```

### Verifying Connection

Before performing operations, you can verify the connection to the FTP server:

```swift
do {
    let isConnected = try await ftpClient.verifyConnection()
    if isConnected {
        print("Successfully connected to the FTP server")
    } else {
        print("Failed to connect to the FTP server")
    }
} catch {
    print("An error occurred while verifying the connection: \(error)")
}
```

## API Reference

### FTPCredentials

A struct representing the credentials needed to connect to an FTP server.

```swift
public struct FTPCredentials {
    let host: String
    let port: UInt16
    let username: String
    let password: String
}
```

### FTPUploadable

An enum representing the types of data that can be uploaded via FTP.

```swift
public enum FTPUploadable {
    case file(url: URL, remoteFileName: String)
    case data(data: Data, remoteFileName: String)
}
```

### FTPClient

The main class for interacting with FTP servers.

```swift
public class FTPClient {
    public init(credentials: FTPCredentials, remotePath: String, progress: Progress? = nil, bufferSize: Int = 512 * 1024)
    
    public func upload(files: [FTPUploadable], progressHandler: @escaping (Progress) -> Void) async throws
    
    public func upload(files: [FTPUploadable], progressHandler: @escaping (Progress) -> Void, completionHandler: @escaping (Result<Void, FTPError>) -> Void)
    
    public func cancel()
    
    public func verifyConnection() async throws -> Bool
}
```

## Error Handling

SwiftFTPClient uses a custom `FTPError` type for error handling:

```swift
public enum FTPError: Error {
    case connectionFailed(String)
    case authenticationFailed(String)
    case transferFailed(String)
    case cancelled
    case other(String)
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

SwiftFTPClient is available under the MIT license. See the LICENSE file for more info.
