//
//  PBFExperiment.swift
//  GITProb
//
//  Created by Nikolay Dinkov on 16.12.21.
//

import Foundation
import SwiftProtobuf


struct FileOpener {
    let fileManager: FileManager = .default
    
    func openFile() {
        guard let fileURL = Bundle.main.url(forResource: "bulgaria-211215.osm", withExtension: ".pbf") else {
            assert(false)
            return
        }
        
        var fileData: Data
        do {
            fileData = try Data(contentsOf: fileURL)
            var parser = Parser(data: fileData)
            var offset: Int = 0
            
            while parser.data.count > 0 {
                let headerLength = Int(parser.parseLEUInt32()!)
                let headerRange = 0..<headerLength
                let blobHeader = try OSMPBF_BlobHeader(serializedData: parser.data.subdata(in: headerRange))
                let blobRange = headerLength ..< (Int(blobHeader.datasize) + headerLength)
                let blob = try OSMPBF_Blob(serializedData: parser.data.subdata(in: blobRange))
                let compressedData = blob.zlibData.dropFirst(2)
                let decompressedData = try (compressedData as NSData).decompressed(using: .zlib)
                switch blobHeader.type {
                case "OSMHeader":
                    let headerBlock = try OSMPBF_HeaderBlock(serializedData: decompressedData as Data)
                    print("Header")
                case "OSMData":
                    let primitiveBlock = try OSMPBF_PrimitiveBlock(serializedData: decompressedData as Data)
//                    if let ways = primitiveBlock.primitivegroup.first?.ways {
//                    print("\nPrimitivegroup - size: \(primitiveBlock.primitivegroup.count)")
                    for var primitiveGroup in primitiveBlock.primitivegroup {
//                        print(primitiveGroup.dense.keysVals)
//                        print(" ")
//                        print(" ")
//                        var count = -1
//                        print(primitiveBlock.stringtable.s.map({ string -> String in
//                            count += 1
//                            return "\(count): \(String(data: string, encoding: .utf8)!)"
//                        }))
//                        print(" ")
//                        print(" ")
                        /// Handle nodes(DenseNode) and print
                        let keyValArray = primitiveGroup.dense.keysVals.reduce([[Int32]]()) { partialResult, current in
                            if current != 0 {
                                var partialResultLast = partialResult.last ?? []
                                partialResultLast.append(current)
                                var partialResultMy = partialResult
                                
                                if partialResult.count == 0 {
                                    partialResultMy = [partialResultLast]
                                } else {
                                    partialResultMy[partialResult.count - 1] = partialResultLast
                                }
                                
                                return partialResultMy
                            } else {
                                var partialResultMy = partialResult
                                partialResultMy.append([])
                                
                                return partialResultMy
                            }
                        }
                        
                        let idKV = zip(primitiveGroup.dense.id, keyValArray)
                        let latLon = zip(primitiveGroup.dense.lat, primitiveGroup.dense.lon)
                        
                        let flattened = zip(idKV, latLon).map { tupleOld in
                            return (tupleOld.0.0, tupleOld.0.1, tupleOld.1.0, tupleOld.1.1)
                        }
                        var counter = 0
                        var result = 0
                        for (id, keyValsArray, lat, lon) in flattened {
                            if counter == 0 {
                                result = Int(id)
                                print("\nNode \(result)")
                            } else {
                                result = result + Int(id)
                                print("\nNode \(result)")
                            }
                            print("\(lat) - \(lon)")
                            for (index, keyVals) in keyValsArray.enumerated() {
                                if index % 2 != 0 {
                                    let keyString = String(data: primitiveBlock.stringtable.s[Int(keyValsArray[index - 1])], encoding: .utf8)!
                                    let valString = String(data: primitiveBlock.stringtable.s[Int(keyVals)], encoding: .utf8)!
                                    print("\(keyString) - \(valString)")
                                }
                            }
                            counter += 1
                        }
                        /// Handle ways and print their values
                        for way in primitiveGroup.ways {
                            var printer = true
//                            for way in ways {
//                            for (key,value) in zip(way.keys, way.vals) {
//                                let keyString = String(data: primitiveBlock.stringtable.s[Int(key)], encoding: .utf8)!
//                                let valString = String(data: primitiveBlock.stringtable.s[Int(value)], encoding: .utf8)!
//                                if keyString.elementsEqual("name") && valString.elementsEqual("бул. Цар Борис III") {
//                                    printer = true
//                                }
//                            }
                            if printer == true {
                                print("\nWay \(way.id)")
                                for (key, value) in zip(way.keys, way.vals) {
                                    let keyString = String(data: primitiveBlock.stringtable.s[Int(key)], encoding: .utf8)!
                                    let valString = String(data: primitiveBlock.stringtable.s[Int(value)], encoding: .utf8)!
    //                                if(valString.elementsEqual("бул. Цар Борис III")) {
    //                                    print("\(keyString) - \(valString)")
    //                                }
                                    print("\(keyString) - \(valString)")
                                }
                            }
                        }
                    }
                default:
                    print("Bad")
                }
                parser.data = parser.data.dropFirst(headerLength + Int(blobHeader.datasize))
            }
            
            print("End")
            
//            fileData = try Data(contentsOf: fileURL)
//            var parser = Parser(data: fileData)
//            let headerLength = Int(parser.parseLEUInt32()!)
//            var headerRange = 0..<headerLength
//            let blobHeader = try OSMPBF_BlobHeader(serializedData: parser.data.subdata(in: headerRange))
////            print(blobHeader)
//            let blobRange = headerLength ..< (Int(blobHeader.datasize) + headerLength)
//            let blob = try OSMPBF_Blob(serializedData: parser.data.subdata(in: blobRange))
//
////            print(blob)
////            let zlibDecomData = try Data(blob.zlibData.decompress(algorithm: NSData.CompressionAlgorithm))
//            let compressedData = blob.zlibData.dropFirst(2)
//            let decompressedData = try (compressedData as NSData).decompressed(using: .zlib)
////            print(decompressedData)
//            let osmHeaderBlock = try OSMPBF_HeaderBlock(serializedData: decompressedData as Data)
//            print(osmHeaderBlock)
//
//            /// Here starts the second BlobHeader
//            parser.data = Data(parser.data.dropFirst(headerLength + Int(blobHeader.datasize)))
//            let nextHeaderLength = Int(parser.parseLEUInt32()!)
////            print(nextHeaderLength)
//            headerRange = 0..<nextHeaderLength
//            let newBlobHeader = try OSMPBF_BlobHeader(serializedData: parser.data.subdata(in: headerRange))
////            print(newBlobHeader)
//            let newBlobRange = nextHeaderLength ..< (Int(newBlobHeader.datasize) + nextHeaderLength)
//            let newBlob = try OSMPBF_Blob(serializedData: parser.data.subdata(in: newBlobRange))
////            print(newBlob)
//            let newCompressedData = newBlob.zlibData.dropFirst(2)
//            let newDecompressedData = try (newCompressedData as NSData).decompressed(using: .zlib)
//            let osmPrimitive = try OSMPBF_PrimitiveBlock(serializedData: newDecompressedData as Data)
//            print(osmPrimitive)
//
//            ///Here starts the third BlobHeader
//            parser.data = Data(parser.data.dropFirst(nextHeaderLength + Int(newBlobHeader.datasize)))
//            let headerLength3 = Int(parser.parseLEUInt32()!)
//            headerRange = 0..<headerLength3
//            let blobHeader3 = try OSMPBF_BlobHeader(serializedData: parser.data.subdata(in: headerRange))
//            let blobRange3 = headerLength3 ..< (Int(blobHeader3.datasize) + headerLength3)
//            let blob3 = try OSMPBF_Blob(serializedData: parser.data.subdata(in: blobRange3))
//            print(blobHeader3)
//            let compressedData3 = blob3.zlibData.dropFirst(2)
//            let decompressedData3 = try (compressedData3 as NSData).decompressed(using: .zlib)
//            let osmNewPrimitive = try OSMPBF_PrimitiveBlock(serializedData: decompressedData3 as Data)
//            print(osmNewPrimitive)
//
//
//
//
//
//
//
//
//
//            fileData = try Data(contentsOf: fileURL)
//            let parser = Parser(data: fileData)
//            var offset: Int = 0
//            while true {
//                if offset >= fileData.count {
//                    break;
//                }
//                let headerLength = Int(parser.parseLEUInt32()!)
//                let headerRange = offset..<offset + headerLength
//                let blobHeader = try OSMPBF_BlobHeader(serializedData: parser.data.subdata(in: headerRange))
//                let blobRange = headerLength ..< (Int(blobHeader.datasize) + headerLength)
//                let blob = try OSMPBF_Blob(serializedData: parser.data.subdata(in: blobRange))
//                let compressedData = blob.zlibData.dropFirst(2)
//                let decompressedData = try (compressedData as NSData).decompressed(using: .zlib)
//                switch blobHeader.type {
//                case "OSMHeader":
//                    let headerBlock = try OSMPBF_HeaderBlock(serializedData: decompressedData as Data)
//                    print(headerBlock)
//                case "OSMData":
//                    let primitiveBlock = try OSMPBF_PrimitiveBlock(serializedData: decompressedData as Data)
//                    print(primitiveBlock)
//                default:
//                    print("Bad")
//                }
//                offset += headerLength + Int(blobHeader.datasize)
//            }
        } catch {
//            print((error as NSError).userInfo)
            print(error.localizedDescription)
            assert(false)
            return
        }
        func printData() {
            //
        }
    }
}


struct Parser {
    var data: Data
    init(data: Data) {
        self.data = data
    }
    
    private mutating func parseLEUIntX<Result>(_: Result.Type) -> Result?
    where Result: UnsignedInteger // May be Signed or Signed Integer if needed
    {
        let expected = MemoryLayout<Result>.size
        guard data.count >= expected else { return nil }
        defer { self.data = Data(self.data.dropFirst(expected)) }
        
        
        return data
            .prefix(expected)
            .reduce(0, { soFar, new in
                (soFar << 8) | Result(new)
            })
    }
    mutating func parseLEUInt8() -> UInt8? {
        parseLEUIntX(UInt8.self)
    }
    
    mutating func parseLEUInt16() -> UInt16? {
        parseLEUIntX(UInt16.self)
    }
    
    mutating func parseLEUInt32() -> UInt32? {
        parseLEUIntX(UInt32.self)
    }
    
    mutating func parseLEUInt64() -> UInt64? {
        parseLEUIntX(UInt64.self)
    }
}
