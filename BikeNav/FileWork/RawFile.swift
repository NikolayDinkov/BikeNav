//
//  PBFExperiment.swift
//  GITProb
//
//  Created by Nikolay Dinkov on 16.12.21.
//

import Foundation
import SwiftProtobuf


class RawFile {
    let serialSyncQueue = DispatchQueue(label: "serial.sync", qos: .userInteractive)
    let parallelProcessingQueue = DispatchQueue(label: "parallel.dictionary", qos: .userInteractive, attributes: [.concurrent])
    let fileManager: FileManager = .default
    var nodes: [DenseNodeNew] = [DenseNodeNew]()
    var ways: [WayNew] = [WayNew]()
    var references: [Int: [WayNew]] = [Int: [WayNew]]()
    
    func openFile() {
        guard let fileURL = Bundle.main.url(forResource: "bulgaria-140101.osm", withExtension: ".pbf") else {
            assert(false)
            return
        }
        let before = Date().timeIntervalSince1970
        var fileData: Data
        do {
            fileData = try Data(contentsOf: fileURL)
            var parser = Parser(data: fileData)
            var offset = 0
            while parser.data.count > 0 {
                let headerLength = Int(parser.parseLEUInt32()!)
                let headerRange = 0..<headerLength
                let blobHeader = try OSMPBF_BlobHeader(serializedData: parser.data.subdata(in: headerRange))
                let blobRange = headerLength ..< (Int(blobHeader.datasize) + headerLength)
                let blob = try OSMPBF_Blob(serializedData: parser.data.subdata(in: blobRange))
                let compressedData = blob.zlibData.dropFirst(2)
                //zlibData[1...2
                let decompressedData = try (compressedData as NSData).decompressed(using: .zlib)
                switch blobHeader.type {
                case "OSMHeader":
                    let headerBlock = try OSMPBF_HeaderBlock(serializedData: decompressedData as Data)
                    print("Header")
                case "OSMData":
                    let primitiveBlock = try OSMPBF_PrimitiveBlock(serializedData: decompressedData as Data)
                    for var primitiveGroup in primitiveBlock.primitivegroup {
                        /// Handle nodes(DenseNode) and print
                        handleNodes(primitiveGroup: primitiveGroup, latOffset: primitiveBlock.latOffset, lonOffset: primitiveBlock.lonOffset, granularity: primitiveBlock.granularity)
                        /// Handle ways and print their values
                        handleWays(primitiveGroup: primitiveGroup, stringTable: primitiveBlock.stringtable)
                    }
                default:
                    print("Bad")
                }
                parser.data = parser.data.dropFirst(headerLength + Int(blobHeader.datasize))
            }
            nodes = nodes.filter({ references[$0.id] != nil })
            print("File read")
            let after = Date().timeIntervalSince1970
            print("Took \((after - before)) seconds")
        } catch {
//            print((error as NSError).userInfo)
            print(error.localizedDescription)
            assert(false)
            return
        }
    }
    
    func handleNodes(primitiveGroup: OSMPBF_PrimitiveGroup, latOffset: Int64, lonOffset: Int64, granularity: Int32) {
        let keyValArray = primitiveGroup.dense.keysVals.reduce([[Int32]]()) { partialResult, current in // TODO: See if I need key and values of Nodes
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
        var deltaDecoderID = DeltaDecoder(previous: 0)
        var deltaDecoderLat = DeltaDecoder(previous: 0)
        var deltaDecoderLon = DeltaDecoder(previous: 0)
        
        for (id, keyValsArray, lat, lon) in flattened {
            deltaDecoderID.previous = deltaDecoderID.previous + Int(id)
            deltaDecoderLat.previous = deltaDecoderLat.previous + Int(lat)
            deltaDecoderLon.previous = deltaDecoderLon.previous + Int(lon)
            
            let newNode = DenseNodeNew(id: deltaDecoderID.previous,
                                       latCalculated: 0.000000001 * Double((Int(latOffset) + (Int(granularity) * deltaDecoderLat.previous))),
                                       lonCalculated: 0.000000001 * Double((Int(lonOffset) + (Int(granularity) * deltaDecoderLon.previous))))
            nodes.append(newNode)
//                            for (index, keyVals) in keyValsArray.enumerated() {
//                                if index % 2 != 0 {
//                                    let keyString = String(data: primitiveBlock.stringtable.s[Int(keyValsArray[index - 1])], encoding: .utf8)!
//                                    let valString = String(data: primitiveBlock.stringtable.s[Int(keyVals)], encoding: .utf8)!
//                                    print("\(keyString) - \(valString)")
//                                }
//                            }
        }
    }
    
    func handleWays(primitiveGroup: OSMPBF_PrimitiveGroup, stringTable: OSMPBF_StringTable) {
        var deltaDecoderID = DeltaDecoder(previous: 0)
        for way in primitiveGroup.ways {
//            let before = Date().timeIntervalSince1970
            var shouldAppend = false
            var keyVal = [String: String]()
            for (key, value) in zip(way.keys, way.vals) {
                let keyString = String(data: stringTable.s[Int(key)], encoding: .utf8)!
                if keyString == "highway" {
                    shouldAppend = true
                }
                let valString = String(data: stringTable.s[Int(value)], encoding: .utf8)!
                keyVal[keyString] = valString
            }
            
            deltaDecoderID.recall()
            var nodeRefs = [Int]()
            for ref in way.refs {
                deltaDecoderID.previous = deltaDecoderID.previous + Int(ref)
                nodeRefs.append(deltaDecoderID.previous)
            }
            if shouldAppend == true {
                let newWay = WayNew(id: Int(way.id), keyVal: keyVal, nodeRefs: nodeRefs)
                ways.append(newWay)
                for id in newWay.nodeRefs {
                    if references[id] == nil {
                        references[id] = [newWay]
                    } else {
                        references[id]!.append(newWay)
                    }
                }
            }
//            let after = Date().timeIntervalSince1970
//            print("Took \((after - before) / 1000) seconds")
        }
    }
    
    func reduceMap() {
        print("Reducing nodes & ways") // FIXME: what if road has turns but its the same way and node represent the turn
        
        let referencesCount = references.keys.count
        let entriesPerTask = 1000
        let tasks = referencesCount / entriesPerTask
        var nodeIdsToRemove: [Int] = []
        
        print("References: \(referencesCount)")
        print("Tasks: \(tasks)")
        
        let before = Date().timeIntervalSince1970
        self.parallelProcessingQueue.sync {
            DispatchQueue.concurrentPerform(iterations: tasks) { offset in
                let startIndex = offset * entriesPerTask
                var endIndex = startIndex + entriesPerTask // FIXME: Calculate the max final index
                if endIndex > self.references.keys.count {
                    endIndex = self.references.keys.count
                }
                let nodeIds = Array(self.references.keys)[startIndex..<endIndex]
                
                var currentIterrationToRemove: [Int] = []
                
                for id in nodeIds {
                    guard let ways = self.references[id] else {
                        print("Node with no ways")
                        continue
                    }
                    
                    guard ways.count > 1 else {
                        currentIterrationToRemove.append(id)
                        continue
                    }

                    let names = Set(ways.compactMap { $0.keyVal["name"] })
                    print(names)
                    if names.count < 2 {
                        currentIterrationToRemove.append(id)
                    }
                }
                
                self.serialSyncQueue.async { // Question: Is that correct, .sync?
                    nodeIdsToRemove.append(contentsOf: currentIterrationToRemove)
                }
            }
        }
        let after = Date().timeIntervalSince1970
        print("Took \((after - before)) seconds")
        // TODO: remove nodes from nodeIdsToRemove | Maybe use threads | Can we run this in the background
        for nodeId in nodeIdsToRemove {
            nodes = nodes.filter({ $0.id != nodeId })
            references[nodeId] = nil
        }
        print("Done deleting not needed nodes")
    }
}

struct DeltaDecoder {
    var previous: Int
    
    init(previous: Int) {
        self.previous = previous
    }
    
    public mutating func recall() {
        self.previous = 0
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
