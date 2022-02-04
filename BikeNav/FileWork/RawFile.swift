//
//  PBFExperiment.swift
//  GITProb
//
//  Created by Nikolay Dinkov on 16.12.21.
//

import Foundation
import SwiftProtobuf


struct RawFile {
    let fileManager: FileManager = .default
    var nodes: [DenseNodeNew] = [DenseNodeNew]()
    var ways: [WayNew] = [WayNew]()
    
    mutating func openFile() {
        guard let fileURL = Bundle.main.url(forResource: "bulgaria-211215.osm", withExtension: ".pbf") else {
            assert(false)
            return
        }
        
        var fileData: Data
        do {
            fileData = try Data(contentsOf: fileURL)
            var deltaDecoderID = DeltaDecoder(previous: 0, current: 0)
            var parser = Parser(data: fileData)            
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
                    for var primitiveGroup in primitiveBlock.primitivegroup {
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
                        var deltaDecoderLat = DeltaDecoder(previous: 0, current: 0)
                        var deltaDecoderLon = DeltaDecoder(previous: 0, current: 0)
                        
                        deltaDecoderID.recall()
                        deltaDecoderLat.recall()
                        deltaDecoderLon.recall()
                        
                        for (id, keyValsArray, lat, lon) in flattened {
                            deltaDecoderID.current = deltaDecoderID.previous + Int(id)
                            deltaDecoderID.previous = deltaDecoderID.current
                            
                            deltaDecoderLat.current = deltaDecoderLat.previous + Int(lat)
                            deltaDecoderLat.previous = deltaDecoderLat.current
                            
                            deltaDecoderLon.current = deltaDecoderLon.previous + Int(lon)
                            deltaDecoderLon.previous = deltaDecoderLon.current
                            
                            print("\nNode \(deltaDecoderID.current)")
                            print("\(0.000000001 * Double((Int(primitiveBlock.latOffset) + (Int(primitiveBlock.granularity) * deltaDecoderLat.current)))) - \(0.000000001 * Double((Int(primitiveBlock.lonOffset) + (Int(primitiveBlock.granularity) * deltaDecoderLon.current))))")
                            let newNode = DenseNodeNew(id: deltaDecoderID.current,
                                                       latCalculated: 0.000000001 * Double((Int(primitiveBlock.latOffset) + (Int(primitiveBlock.granularity) * deltaDecoderLat.current))),
                                                       lonCalculated: 0.000000001 * Double((Int(primitiveBlock.lonOffset) + (Int(primitiveBlock.granularity) * deltaDecoderLon.current))))
                            nodes.append(newNode)
//                            for (index, keyVals) in keyValsArray.enumerated() {
//                                if index % 2 != 0 {
//                                    let keyString = String(data: primitiveBlock.stringtable.s[Int(keyValsArray[index - 1])], encoding: .utf8)!
//                                    let valString = String(data: primitiveBlock.stringtable.s[Int(keyVals)], encoding: .utf8)!
//                                    print("\(keyString) - \(valString)")
//                                }
//                            }
                        }
                        /// Handle ways and print their values
                        for way in primitiveGroup.ways {
//                            var printer = false
//                            for (key,value) in zip(way.keys, way.vals) {
//                                let keyString = String(data: primitiveBlock.stringtable.s[Int(key)], encoding: .utf8)!
//                                let valString = String(data: primitiveBlock.stringtable.s[Int(value)], encoding: .utf8)!
//                                if keyString.elementsEqual("name") && valString.elementsEqual("бул. Цар Борис III") {
//                                    printer = true
//                                }
//                            }
//                            if printer == true {
                            deltaDecoderID.recall()
                            for ref in way.refs {
                                deltaDecoderID.current = deltaDecoderID.previous + Int(ref)
                                print(deltaDecoderID.current)
                                deltaDecoderID.previous = deltaDecoderID.current
                            }
                            for (key, value) in zip(way.keys, way.vals) {
                                let keyString = String(data: primitiveBlock.stringtable.s[Int(key)], encoding: .utf8)!
                                let valString = String(data: primitiveBlock.stringtable.s[Int(value)], encoding: .utf8)!
                                print("\(keyString) - \(valString)")
                            }
//                            }
                        }
                    }
                default:
                    print("Bad")
                }
                parser.data = parser.data.dropFirst(headerLength + Int(blobHeader.datasize))
            }
            print("End")
        } catch {
//            print((error as NSError).userInfo)
            print(error.localizedDescription)
            assert(false)
            return
        }
        
    }
}

struct DeltaDecoder {
    var previous: Int
    var current: Int
    
    init(previous: Int, current: Int) {
        self.previous = previous
        self.current = current
    }
    
    public mutating func recall() {
        self.previous = 0
        self.current = 0
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
