import Foundation
import Network

/// A client for interacting with FTP servers.
///
/// This class provides functionality to connect to an FTP server, upload files or data,
/// and manage the transfer process.
///
/// Example usage:
/// ```swift
/// let credentials = FTPCredentials(host: "ftp.example.com", port: 21, username: "user", password: "pass")
/// let remotePath = "/upload/path"
///
/// let ftpClient = FTPClient(credentials: credentials, remotePath: remotePath)
///
/// let filesToUpload: [FTPUploadable] = [
///     .file(url: URL(fileURLWithPath: "/path/to/local/file1.txt"), remoteFileName: "file1.txt"),
///     .data(data: Data("Sample data".utf8), remoteFileName: "sample.txt")
/// ]
///
/// ftpClient.upload(files: filesToUpload, progressHandler: { progress in
///     print("Overall progress: \(progress.fractionCompleted * 100)%")
/// }, completionHandler: { result in
///     switch result {
///     case .success:
///         print("All files uploaded successfully.")
///     case .failure(let error):
///         print("An error occurred: \(error)")
///     }
/// })
/// ```
public class FTPClient {
    private let credentials: FTPCredentials
    private let remotePath: String
    private var controlConnection: NWConnection?
    private var isCancelled = false
    private var progress: Progress?
    private let bufferSize: Int

    /// Initializes a new FTP client.
    /// - Parameters:
    ///   - credentials: The credentials for connecting to the FTP server.
    ///   - remotePath: The remote path on the server where files will be uploaded.
    public init(
        credentials: FTPCredentials,
        remotePath: String,
        progress: Progress? = nil,
        bufferSize: Int = 512 * 1024
    ) {
        self.credentials = credentials
        self.remotePath = remotePath
        self.progress = progress
        self.bufferSize = bufferSize
    }

    // MARK: - Public Methods

    /// Uploads multiple files to the FTP server.
    /// - Parameters:
    ///   - files: An array of `FTPUploadable` items to be uploaded.
    ///   - progressHandler: A closure that is called with updates on the overall progress of the upload.
    ///   - completionHandler: A closure that is called when all uploads are complete, or if an error occurs.
    public func upload(
        files: [FTPUploadable],
        progressHandler: @escaping (Progress) -> Void,
        completionHandler: @escaping (Result<Void, FTPError>) -> Void
    ) {
        Task {
            do {
                try await upload(files: files, progressHandler: progressHandler)
                completionHandler(.success(()))
            } catch let error as FTPError {
                completionHandler(.failure(error))
            } catch {
                completionHandler(.failure(.other(error.localizedDescription)))
            }
        }
    }

    /// Cancels any ongoing transfer operations.
    public func cancel() {
        isCancelled = true
        controlConnection?.cancel()
    }

    // MARK: - Internal async upload

    public func upload(
        files: [FTPUploadable],
        progressHandler: @escaping (Progress) -> Void
    ) async throws {
        // Connect and authenticate
        try await connect()

        // Calculate total size for Progress
        let totalSize = try files.reduce(Int64(0)) { result, uploadable in
            switch uploadable {
            case .file(let url, _):
                let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
                let fileSize = attributes[.size] as? Int64 ?? 0
                return result + fileSize
            case .data(let data, _):
                return result + Int64(data.count)
            }
        }

        let progress = self.progress ?? Progress(totalUnitCount: totalSize)
        progressHandler(progress)

        for uploadable in files {
            if isCancelled {
                throw FTPError.cancelled
            }

            switch uploadable {
            case .file(let url, let remoteFileName):
                try await uploadFile(
                    url: url,
                    remoteFileName: remoteFileName,
                    progress: progress,
                    progressHandler: progressHandler
                )
            case .data(let data, let remoteFileName):
                try await uploadData(
                    data: data,
                    remoteFileName: remoteFileName,
                    progress: progress,
                    progressHandler: progressHandler
                )
            }
        }

        // Close control connection
        controlConnection?.cancel()
    }

    // MARK: - Connection / Commands

    private func connect() async throws {
        let parameters = NWParameters.tcp
        let endpoint = NWEndpoint.Host(credentials.host)
        let port = NWEndpoint.Port(rawValue: credentials.port)!
        controlConnection = NWConnection(host: endpoint, port: port, using: parameters)

        try await withSafeStateHandler { completion in
            controlConnection?.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    completion(.success(()))
                case .failed(let error):
                    completion(.failure(FTPError.connectionFailed(error.localizedDescription)))
                case .cancelled:
                    completion(.failure(FTPError.cancelled))
                default:
                    break
                }
            }
            controlConnection?.start(queue: .global())
        }

        // Read the initial server response
        _ = try await readResponse()

        // USER
        try await sendCommand("USER \(credentials.username)")
        let userResponse = try await readResponse()
        guard userResponse.starts(with: "331") else {
            throw FTPError.authenticationFailed("Username not accepted: \(userResponse)")
        }

        // PASS
        try await sendCommand("PASS \(credentials.password)")
        let passResponse = try await readResponse()
        guard passResponse.starts(with: "230") else {
            throw FTPError.authenticationFailed("Password not accepted: \(passResponse)")
        }

        // CWD
        try await sendCommand("CWD \(remotePath)")
        let cwdResponse = try await readResponse()
        guard cwdResponse.starts(with: "250") else {
            throw FTPError.other("Failed to change to remote directory: \(cwdResponse)")
        }
    }

    private func sendCommand(_ command: String) async throws {
        guard let connection = controlConnection else {
            throw FTPError.connectionFailed("No control connection available.")
        }
        let commandWithCRLF = command + "\r\n"
        guard let data = commandWithCRLF.data(using: .utf8) else {
            throw FTPError.other("Failed to encode command.")
        }

        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            connection.send(content: data, completion: .contentProcessed { error in
                if let error = error {
                    continuation.resume(throwing: FTPError.connectionFailed(error.localizedDescription))
                } else {
                    continuation.resume()
                }
            })
        }
    }

        var completeResponse = ""
        let maxResponseLength = 64 * 1024  // 64 KB safety limit
        while true {
            let partialResponse: String = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<String, Error>) in
                connection.receive(minimumIncompleteLength: 1, maximumLength: 1024) { data, _, isComplete, error in
                    if let error = error {
                        continuation.resume(throwing: FTPError.connectionFailed(error.localizedDescription))
                    } else if let data = data, let response = String(data: data, encoding: .utf8) {
                        continuation.resume(returning: response)
                    } else if isComplete {
                        continuation.resume(throwing: FTPError.other("Connection closed while reading response."))
                    } else {
                        continuation.resume(throwing: FTPError.other("Failed to read response from server."))
                    }
                }
            }

            completeResponse += partialResponse

            // Safety: avoid unbounded growth if the server sends malformed or
            // extremely long responses (which can lead to huge allocations).
            if completeResponse.count > maxResponseLength {
                throw FTPError.other("Server response too long or malformed: \(completeResponse.prefix(200))...")
            }

            // Check if response is complete (ends with \r\n)
            if completeResponse.hasSuffix("\r\n") {
                break
            }
        }
        return completeResponse

    // MARK: - Upload helpers

    private func uploadFile(
        url: URL,
        remoteFileName: String,
        progress: Progress,
        progressHandler: @escaping (Progress) -> Void
    ) async throws {
        let fileHandle = try FileHandle(forReadingFrom: url)
        defer {
            try? fileHandle.close()
        }

        // PASV and data connection
        let dataConnection = try await enterPassiveModeAndOpenDataConnection()

        // STOR
        try await sendCommand("STOR \(remoteFileName)")
        let storResponse = try await readResponse()
        guard storResponse.starts(with: "150") else {
            throw FTPError.transferFailed("Failed to initiate file transfer: \(storResponse)")
        }

        // Send file data in chunks using readData(ofLength:), which works on older iOS versions too.
        while true {
            if isCancelled {
                dataConnection.cancel()
                throw FTPError.cancelled
            }

            let chunk = fileHandle.readData(ofLength: bufferSize)
            if chunk.isEmpty {
                break
            }

            try await sendData(data: chunk, over: dataConnection)
            progress.completedUnitCount += Int64(chunk.count)
            progressHandler(progress)
        }

        // Close data connection
        dataConnection.cancel()

        // Read final server response
        let transferResponse = try await readResponse()
        guard transferResponse.starts(with: "226") else {
            throw FTPError.transferFailed("File transfer failed: \(transferResponse)")
        }
    }

    private func uploadData(
        data: Data,
        remoteFileName: String,
        progress: Progress,
        progressHandler: @escaping (Progress) -> Void
    ) async throws {
        // PASV
        let dataConnection = try await enterPassiveModeAndOpenDataConnection()

        // STOR
        try await sendCommand("STOR \(remoteFileName)")
        let storResponse = try await readResponse()
        guard storResponse.starts(with: "150") else {
            throw FTPError.transferFailed("Failed to initiate data transfer: \(storResponse)")
        }

        // Send data
        try await sendData(data: data, over: dataConnection)
        progress.completedUnitCount += Int64(data.count)
        progressHandler(progress)

        // Close data connection
        dataConnection.cancel()

        // Read final response
        let transferResponse = try await readResponse()
        guard transferResponse.starts(with: "226") else {
            throw FTPError.transferFailed("Data transfer failed: \(transferResponse)")
        }
    }

    private func enterPassiveModeAndOpenDataConnection() async throws -> NWConnection {
        try await sendCommand("PASV")
        let pasvResponse = try await readResponse()

        // Parse (h1,h2,h3,h4,p1,p2)
        let pattern = "\\((.*?)\\)"
        let regex = try NSRegularExpression(pattern: pattern)
        guard let match = regex.firstMatch(
            in: pasvResponse,
            range: NSRange(pasvResponse.startIndex..., in: pasvResponse)
        ) else {
            throw FTPError.other("Failed to parse PASV response: \(pasvResponse)")
        }

        let range = Range(match.range(at: 1), in: pasvResponse)!
        let numbersString = pasvResponse[range]
        let numbers = numbersString
            .split(separator: ",")
            .compactMap { UInt16($0.trimmingCharacters(in: .whitespaces)) }

        guard numbers.count == 6 else {
            throw FTPError.other("Invalid PASV response format: \(pasvResponse)")
        }

        let host = "\(numbers[0]).\(numbers[1]).\(numbers[2]).\(numbers[3])"
        let port = (numbers[4] << 8) + numbers[5]

        let dataConnection = NWConnection(
            host: NWEndpoint.Host(host),
            port: NWEndpoint.Port(rawValue: port)!,
            using: .tcp
        )

        try await withSafeStateHandler { completion in
            dataConnection.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    completion(.success(()))
                case .failed(let error):
                    completion(.failure(FTPError.connectionFailed(error.localizedDescription)))
                case .cancelled:
                    completion(.failure(FTPError.cancelled))
                default:
                    break
                }
            }
            dataConnection.start(queue: .global())
        }

        return dataConnection
    }

    private func sendData(data: Data, over connection: NWConnection) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            connection.send(content: data, completion: .contentProcessed { error in
                if let error = error {
                    continuation.resume(throwing: FTPError.transferFailed(error.localizedDescription))
                } else {
                    continuation.resume()
                }
            })
        }
    }

    // MARK: - Connection verification

    /// Verifies the connection to the FTP server.
    /// This method attempts to connect to the server, authenticate, and then disconnect.
    public func verifyConnection() async throws -> Bool {
        do {
            try await connect()
            await disconnect()
            return true
        } catch {
            throw error
        }
    }

    private func disconnect() async {
        controlConnection?.cancel()
        controlConnection = nil
    }

    // MARK: - Safe continuation helper

    private actor SafeCompletionHandler<T> {
        private var hasCompleted = false

        func complete(with result: Result<T, Error>, continuation: CheckedContinuation<T, Error>) {
            guard !hasCompleted else { return }
            hasCompleted = true
            continuation.resume(with: result)
        }
    }

    private func withSafeStateHandler<T>(
        _ operation: (@escaping (Result<T, Error>) -> Void) -> Void
    ) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            let safeHandler = SafeCompletionHandler<T>()
            operation { result in
                Task {
                    await safeHandler.complete(with: result, continuation: continuation)
                }
            }
        }
    }
}
