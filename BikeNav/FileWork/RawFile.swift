//
//  PBFExperiment.swift
//  GITProb
//
//  Created by Nikolay Dinkov on 16.12.21.
//

import Foundation
import SwiftProtobuf


class RawFile {
    var primitiveBLocks: [OSMPBF_PrimitiveBlock] = [OSMPBF_PrimitiveBlock]()
    
    let serialSyncQueue = DispatchQueue(label: "serial.sync", qos: .userInteractive)
    let parallelProcessingQueue = DispatchQueue(label: "parallel.dictionary", qos: .userInteractive, attributes: [.concurrent])
    let fileManager: FileManager = .default
    var nodes: [DenseNodeNew] = [DenseNodeNew]()
    var ways: [WayNew] = [WayNew]()
    var references: [Int: [WayNew]] = [Int: [WayNew]]()
    
    func parseFile() {
        guard let fileURL = Bundle.main.url(forResource: "bulgaria-140101.osm", withExtension: ".pbf") else {
            assert(false)
            return
        }
        let before = Date().timeIntervalSince1970

        var fileData: Data
        do {
//            let before = Date().timeIntervalSince1970
            fileData = try Data(contentsOf: fileURL)
            var parser = Parser(data: fileData)
            var offset = 0
            while parser.data.count > 0 { // MARK: Cannot use threads for the reading, but can for handling
//            while offset < parser.data.count {
                let headerLength = Int(parser.parseLEUInt32()!)
//                offset += 4
                let headerRange = offset ..< (headerLength + offset)
                let blobHeader = try OSMPBF_BlobHeader(serializedData: parser.data.subdata(in: headerRange))
                let blobRange = (headerLength + offset) ..< (Int(blobHeader.datasize) + headerLength + offset)
                let blob = try OSMPBF_Blob(serializedData: parser.data.subdata(in: blobRange))
                let compressedData = blob.zlibData.dropFirst(2) // blob.zlibData.subdata(in: 2..<blob.zlibData.count)
                //zlibData[1...2
                let decompressedData = try (compressedData as NSData).decompressed(using: .zlib)
                switch blobHeader.type {
                case "OSMHeader":
                    let headerBlock = try OSMPBF_HeaderBlock(serializedData: decompressedData as Data)
                    print("Header")
                case "OSMData":
                    let primitiveBlock = try OSMPBF_PrimitiveBlock(serializedData: decompressedData as Data)
                    primitiveBLocks.append(primitiveBlock)
//                    for var primitiveGroup in primitiveBlock.primitivegroup {
//                        /// Handle nodes(DenseNode) and print
//                        handleNodes(primitiveGroup: primitiveGroup, latOffset: primitiveBlock.latOffset, lonOffset: primitiveBlock.lonOffset, granularity: primitiveBlock.granularity, stringTable: primitiveBlock.stringtable)
//                        /// Handle ways and print their values
//                        handleWays(primitiveGroup: primitiveGroup, stringTable: primitiveBlock.stringtable)
//                    }
                default:
                    print("Bad")
                }
                parser.data = parser.data.dropFirst(headerLength + Int(blobHeader.datasize))
//                offset = offset + headerLength + Int(blobHeader.datasize)
            }
            if let idx = nodes.firstIndex(where: { references[$0.id] == nil }) {
                nodes.remove(at: idx)
            }
//            nodes = nodes.filter({ references[$0.id] != nil })
            print("File opened")
            let after = Date().timeIntervalSince1970
            print("Took \((after - before)) seconds")
        } catch {
//            print((error as NSError).userInfo)
            print(error.localizedDescription)
            assert(false)
            return
        }
    }
    
    func readFile() {
        print("Reading the file")
        let primitiveBlocksCount = primitiveBLocks.count
        let entriesPerTask = 70
        let tasks = primitiveBlocksCount / entriesPerTask
        print("References: \(primitiveBlocksCount)")
        print("Tasks: \(tasks)")
        
        self.parallelProcessingQueue.sync {
            DispatchQueue.concurrentPerform(iterations: tasks) { offset in
                let startIndex = offset * entriesPerTask
                let endIndex: Int
                if offset == tasks - 1 {
                    endIndex = startIndex + (primitiveBlocksCount - (entriesPerTask * tasks) + entriesPerTask)
                } else {
                    endIndex = startIndex + entriesPerTask
                }
                let primitiveBlocksForIteration = primitiveBLocks[startIndex..<endIndex]
                
                var nodesToAdd = [DenseNodeNew]()
                var waysToAdd = [WayNew]()
                var dictToAdd = [Int: [WayNew]]()
                
                for var primitiveBlock in primitiveBlocksForIteration { // MARK: ADD needed things
                    for var primitiveGroup in primitiveBlock.primitivegroup { // MARK: Care when using and synchronizing the threads
                        let nodesReturned = handleNodes(primitiveGroup: primitiveGroup, latOffset: primitiveBlock.latOffset, lonOffset: primitiveBlock.lonOffset, granularity: primitiveBlock.granularity, stringTable: primitiveBlock.stringtable)
                        nodesToAdd.append(contentsOf: nodesReturned)
                        let arrayDictAppender = handleWays(primitiveGroup: primitiveGroup, stringTable: primitiveBlock.stringtable)
                        waysToAdd.append(contentsOf: arrayDictAppender.waysForAppending)
                        dictToAdd.merge(arrayDictAppender.dictForAppending) { <#[WayNew]#>, <#[WayNew]#> in
                            <#code#>
                        }
                    }
                }
                self.serialSyncQueue.async {
                    self.nodes.append(contentsOf: nodesToAdd)
                    self.ways.append(contentsOf: waysToAdd)
                    self.references.merge(dictToAdd) { <#[WayNew]#>, <#[WayNew]#> in
                        <#code#>
                    }
                }
            }
        }
    }
    
    func handleNodes(primitiveGroup: OSMPBF_PrimitiveGroup, latOffset: Int64, lonOffset: Int64, granularity: Int32, stringTable: OSMPBF_StringTable) -> [DenseNodeNew] { // TODO: See if I need key and values of Nodes
        let beforeW = Date().timeIntervalSince1970
        var handledNodes = [DenseNodeNew]()
//        let keyValArray = primitiveGroup.dense.keysVals.reduce([[Int32]]()) { partialResult, current in
//            if current != 0 {
//                var partialResultLast = partialResult.last ?? []
//                partialResultLast.append(current)
//                var partialResultMy = partialResult
//                if partialResult.count == 0 {
//                    partialResultMy = [partialResultLast]
//                } else {
//                    partialResultMy[partialResult.count - 1] = partialResultLast
//                }
//                return partialResultMy
//            } else {
//                var partialResultMy = partialResult
//                partialResultMy.append([])
//                return partialResultMy
//            }
//        }

//        let idKV = zip(primitiveGroup.dense.id, keyValArray)
        let latLon = zip(primitiveGroup.dense.lat, primitiveGroup.dense.lon)

        let flattened = zip(primitiveGroup.dense.id, latLon).map { tupleOld in
            return (tupleOld.0, tupleOld.1.0, tupleOld.1.1)
        }
        var deltaDecoderID = DeltaDecoder(previous: 0) // MARK: Will have problem when using threads
        var deltaDecoderLat = DeltaDecoder(previous: 0)
        var deltaDecoderLon = DeltaDecoder(previous: 0)

        for (id, lat, lon) in flattened {
            deltaDecoderID.previous = deltaDecoderID.previous + Int(id)
            deltaDecoderLat.previous = deltaDecoderLat.previous + Int(lat)
            deltaDecoderLon.previous = deltaDecoderLon.previous + Int(lon)
            
            let newNode = DenseNodeNew(id: deltaDecoderID.previous,
                                       latCalculated: 0.000000001 * Double((Int(latOffset) + (Int(granularity) * deltaDecoderLat.previous))),
                                       lonCalculated: 0.000000001 * Double((Int(lonOffset) + (Int(granularity) * deltaDecoderLon.previous))))
            handledNodes.append(newNode)
//            nodes.append(newNode)
//            print()
//            print(newNode)
//            for (index, keyVals) in keyValsArray.enumerated() {
//                if index % 2 != 0 {
//                    let keyString = String(data: stringTable.s[Int(keyValsArray[index - 1])], encoding: .utf8)!
//                    let valString = String(data: stringTable.s[Int(keyVals)], encoding: .utf8)!
//                    print("\(keyString) - \(valString)")
//                }
//            }
        }
//        let afterW = Date().timeIntervalSince1970
//        print("Took \(afterW - beforeW) seconds")
        return handledNodes
    }
    
    func handleWays(primitiveGroup: OSMPBF_PrimitiveGroup, stringTable: OSMPBF_StringTable) ->  ArrayDictAppender {
//        print("Handling ways")
//        let referencesCount = primitiveGroup.ways.count
//        let entriesPerTask = 1000
//        let tasks = referencesCount / entriesPerTask
        var handledWays = [WayNew]()
        var handledDictionary = [Int: [WayNew]]()
        
//        self.parallelProcessingQueue.sync {
//            DispatchQueue.concurrentPerform(iterations: tasks) { offset in
//                let startIndex = offset * entriesPerTask
//                var endIndex = startIndex + entriesPerTask
//                if endIndex > primitiveGroup.ways.count {
//                    endIndex = primitiveGroup.ways.count
//                }
//                let waysInOffset = primitiveGroup.ways[startIndex..<endIndex]
//                for way in waysInOffset {
//                    var shouldAppend = false
//                    var keyVal = [String: String]()
//                    for (key, value) in zip(way.keys, way.vals) {
//                        let keyString = String(data: stringTable.s[Int(key)], encoding: .utf8)!
//                        if keyString == "highway" {
//                            shouldAppend = true
//                        }
//                        let valString = String(data: stringTable.s[Int(value)], encoding: .utf8)!
//                        keyVal[keyString] = valString
//                    }
//                    var deltaDecoderID = DeltaDecoder(previous: 0)
//                    var nodeRefs = [Int]()
//                    for ref in way.refs {
//                        deltaDecoderID.previous = deltaDecoderID.previous + Int(ref)
//                        nodeRefs.append(deltaDecoderID.previous)
//                    }
//                }
//            }
//        }
        
        for way in primitiveGroup.ways {
//            let beforeW = Date().timeIntervalSince1970
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
            
            var deltaDecoderID = DeltaDecoder(previous: 0)
            var nodeRefs = [Int]()
            for ref in way.refs {
                deltaDecoderID.previous = deltaDecoderID.previous + Int(ref)
                nodeRefs.append(deltaDecoderID.previous)
            }
            if shouldAppend == true {
                let newWay = WayNew(id: Int(way.id), keyVal: keyVal, nodeRefs: nodeRefs)
                handledWays.append(newWay)
                for id in newWay.nodeRefs {
                    if handledDictionary[id] == nil {
                        handledDictionary[id] = [newWay]
                    } else {
                        handledDictionary[id]!.append(newWay)
                    }
                }
            }
//            let afterW = Date().timeIntervalSince1970
//            print("Took \(afterW - beforeW) seconds")
        }
        return ArrayDictAppender(ways: handledWays, dict: handledDictionary)
    }
    
    func reduceMap() {
        print("\nReducing nodes & ways") // FIXME: what if road has turns but its the same way and node represent the turn, or how are we going to navigate if we do not have the distance or way is not finishing in the node
        
        let referencesCount = references.keys.count
        let entriesPerTask = 1000
        let tasks = referencesCount / entriesPerTask
        var nodeIdsToStay: [Int] = []
        
        print("References: \(referencesCount)")
        print("Tasks: \(tasks)")
        
        let before = Date().timeIntervalSince1970
        self.parallelProcessingQueue.sync {
            DispatchQueue.concurrentPerform(iterations: tasks) { offset in
                let startIndex = offset * entriesPerTask
                var endIndex: Int
                if offset == tasks - 1 {
                    endIndex = startIndex + (referencesCount - (entriesPerTask * tasks) + entriesPerTask)
                } else {
                    endIndex = startIndex + entriesPerTask // FIXME: Calculate the max final index | It's ok
                }
                let nodeIds = Array(self.references.keys)[startIndex..<endIndex]
                
                var currentIterrationToStay: [Int] = []
                
                for id in nodeIds {
                    guard let ways = self.references[id] else {
                        print("Node with no ways")
                        continue
                    }
                    
                    let names = Set(ways.compactMap { $0.keyVal["name"] })
//                    print(names)
                    
                    if names.count >= 2, ways.count > 1 { // MARK: ,ways.count > 1 is giving 7 less id's to add | threads not ok
                        currentIterrationToStay.append(id)
                    }
                }
                
                self.serialSyncQueue.async { // MARK: Is that correct, .sync?
                    nodeIdsToStay.append(contentsOf: currentIterrationToStay)
                }
            }
        }
        let after = Date().timeIntervalSince1970
        print("Took \((after - before)) seconds")
        // TODO: remove nodes from nodeIdsToRemove | Maybe use threads | Can we run this in the background | I don't think we can use threads here
        print(nodeIdsToStay.count)
        let before2 = Date().timeIntervalSince1970
        for nodeId in nodeIdsToStay {
//            if let idx = nodes.firstIndex(where: { $0.id == nodeId }) { // MARK: Index getting maybe with threads and then removing from array would be faster
//                nodes.remove(at: idx)
//            }
            nodes = nodes.filter({ $0.id == nodeId })
            references[nodeId] = nil // MARK: This is working the other way around
        }
        let after2 = Date().timeIntervalSince1970
        print("Second calculation \((after2 - before2)) seconds. Before2 = \(before2); After2 = \(after2)")
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
    where Result: UnsignedInteger // May be Signed or UnSigned Integer if needed
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
