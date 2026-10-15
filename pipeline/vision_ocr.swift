// On-device OCR for public document images. No network service or model weights
// are bundled. Requires macOS 26 with Arabic support in the Vision framework.
import Foundation
import Vision
import ImageIO

func pixelBox(_ b: CGRect, _ width: Int, _ height: Int) throws -> [Int] {
    let epsilon = 0.000001
    guard b.minX.isFinite, b.minY.isFinite, b.maxX.isFinite, b.maxY.isFinite,
          b.width > 0, b.height > 0,
          b.minX >= -epsilon, b.minY >= -epsilon,
          b.maxX <= 1 + epsilon, b.maxY <= 1 + epsilon else {
        throw NSError(domain: "AtlasOCR", code: 4, userInfo: [NSLocalizedDescriptionKey: "OCR returned invalid normalized geometry"])
    }
    let x0 = max(0, min(width - 1, Int(floor(b.minX * Double(width)))))
    let y0 = max(0, min(height - 1, Int(floor((1.0 - b.maxY) * Double(height)))))
    let x1 = max(x0 + 1, min(width, Int(ceil(b.maxX * Double(width)))))
    let y1 = max(y0 + 1, min(height, Int(ceil((1.0 - b.minY) * Double(height)))))
    return [x0, y0, x1, y1]
}

func recognize(_ path: String) throws -> [String: Any] {
    let started = Date()
    let url = URL(fileURLWithPath: path)
    guard let source = CGImageSourceCreateWithURL(url as CFURL, nil),
          let image = CGImageSourceCreateImageAtIndex(source, 0, nil) else {
        throw NSError(domain: "AtlasOCR", code: 1, userInfo: [NSLocalizedDescriptionKey: "Image could not be decoded"])
    }
    let request = VNRecognizeTextRequest()
    request.revision = VNRecognizeTextRequestRevision3
    request.recognitionLevel = .accurate
    request.recognitionLanguages = ["ar-SA", "en-US"]
    request.usesLanguageCorrection = false
    request.automaticallyDetectsLanguage = false
    request.minimumTextHeight = 0.0
    let supported = try request.supportedRecognitionLanguages()
    guard supported.contains("ar-SA") else {
        throw NSError(domain: "AtlasOCR", code: 2, userInfo: [NSLocalizedDescriptionKey: "Arabic OCR is not supported on this operating system"])
    }
    let handler = VNImageRequestHandler(cgImage: image, orientation: .up)
    try handler.perform([request])
    var words = [[String: Any]]()
    var lines = [[String: Any]]()
    let pattern = try NSRegularExpression(pattern: "\\S+")
    for observation in request.results ?? [] {
        guard let candidate = observation.topCandidates(1).first, !candidate.string.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { continue }
        let text = candidate.string
        let lineID = "l\(lines.count)"
        var wordIDs = [String]()
        let matches = pattern.matches(in: text, range: NSRange(text.startIndex..., in: text))
        for match in matches {
            guard let range = Range(match.range, in: text) else { continue }
            let wordText = String(text[range])
            let rectangle = try candidate.boundingBox(for: range)
            guard let rectangle = rectangle else {
                throw NSError(domain: "AtlasOCR", code: 5, userInfo: [NSLocalizedDescriptionKey: "OCR word localization was unavailable; page requires review"])
            }
            let wordID = "w\(words.count)"
            wordIDs.append(wordID)
            words.append([
                "id": wordID, "text": wordText,
                "bbox": try pixelBox(rectangle.boundingBox, image.width, image.height),
                "bbox_method": "vision_approximate_character_range_word_precision",
                "bbox_boundary_epsilon_clipped": rectangle.boundingBox.minX < 0 || rectangle.boundingBox.minY < 0 || rectangle.boundingBox.maxX > 1 || rectangle.boundingBox.maxY > 1,
                "confidence": Double(candidate.confidence) * 100.0,
                "confidence_basis": "line_candidate_uncalibrated",
                "line_id": lineID
            ])
        }
        if !wordIDs.isEmpty {
            lines.append([
                "id": lineID, "text": text,
                "bbox": try pixelBox(observation.boundingBox, image.width, image.height),
                "word_ids": wordIDs, "order_index": lines.count,
                "order_method": "apple_vision_observation_order",
                "confidence": Double(candidate.confidence) * 100.0
            ])
        }
    }
    return [
        "width": image.width, "height": image.height,
        "page_text": lines.compactMap { $0["text"] as? String }.joined(separator: "\n"),
        "words": words, "lines": lines,
        "ocr_seconds": Date().timeIntervalSince(started),
        "engine": "Apple Vision VNRecognizeTextRequest",
        "engine_revision": request.revision,
        "operating_system": ProcessInfo.processInfo.operatingSystemVersionString,
        "recognition_languages": request.recognitionLanguages,
        "recognition_level": "accurate",
        "uses_language_correction": request.usesLanguageCorrection,
        "minimum_text_height": request.minimumTextHeight
    ]
}

func emit(_ value: [String: Any]) {
    do {
        let data = try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys, .withoutEscapingSlashes])
        FileHandle.standardOutput.write(data)
        FileHandle.standardOutput.write(Data([10]))
    } catch {
        FileHandle.standardError.write(Data("JSON encoding failed\n".utf8))
    }
}

if CommandLine.arguments.contains("--server") {
    while let input = readLine() {
        autoreleasepool {
            do {
                guard let data = input.data(using: .utf8),
                      let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                      let path = object["path"] as? String else {
                    throw NSError(domain: "AtlasOCR", code: 3, userInfo: [NSLocalizedDescriptionKey: "Expected a JSON object with an image path"])
                }
                emit(["ok": true, "result": try recognize(path)])
            } catch { emit(["ok": false, "error": error.localizedDescription]) }
        }
    }
} else if CommandLine.arguments.count == 2 {
    do { emit(["ok": true, "result": try recognize(CommandLine.arguments[1])]) }
    catch { emit(["ok": false, "error": error.localizedDescription]); exit(1) }
} else {
    FileHandle.standardError.write(Data("Usage: vision_ocr IMAGE | vision_ocr --server\n".utf8))
    exit(2)
}
