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
    var references: [Int: [WayNew]] = [Int: [WayNew]]() //MARK: It could be not int but DenseNodeNew and in it to be for example name later on for the graph and to delete nodes array once we calculate the distance of a way
    
    func readFile() {
        print("Reading the file")
        guard let fileURL = Bundle.main.url(forResource: "bulgaria-211215.osm", withExtension: ".pbf") else {
//        guard let fileURL = Bundle.main.url(forResource: "bulgaria-140101.osm", withExtension: ".pbf") else {

            assert(false)
            return
        }
        let before = Date().timeIntervalSince1970

        let readingGroup = DispatchGroup() // Sync blobs decoding
        var fileData: Data
        do {
            fileData = try Data(contentsOf: fileURL)
            var parser = Parser(data: fileData)
            let offset = 0
            while parser.data.count > 0 { // MARK: Cannot use threads for the reading, but can for handling
                let headerLength = Int(parser.parseLEUInt32()!)
                let headerRange = offset ..< (headerLength + offset)
                let blobHeader = try OSMPBF_BlobHeader(serializedData: parser.data.subdata(in: headerRange))
                let blobRange = (headerLength + offset) ..< (Int(blobHeader.datasize) + headerLength + offset)
                let blob = try OSMPBF_Blob(serializedData: parser.data.subdata(in: blobRange))
                let compressedData = blob.zlibData.dropFirst(2) // blob.zlibData.subdata(in: 2..<blob.zlibData.count)
                let decompressedData = try (compressedData as NSData).decompressed(using: .zlib)
                switch blobHeader.type {
                case "OSMHeader":
//                    let headerBlock = try OSMPBF_HeaderBlock(serializedData: decompressedData as Data)
                    break
                case "OSMData":
                    readingGroup.enter() // Start a task
                    parallelProcessingQueue.async {
                        do {
                            let primitiveBlock = try OSMPBF_PrimitiveBlock(serializedData: decompressedData as Data)
                            self.serialSyncQueue.async {
                                self.primitiveBLocks.append(primitiveBlock)
                                readingGroup.leave() // Finish the task
                            }
                        } catch {
                            print(error.localizedDescription)
                            assert(false)
                            readingGroup.leave() // Task should also be finished in case of an error to allow the execution to continue
                        }
                    }
                default:
                    print("Bad")
                }
                parser.data = parser.data.dropFirst(headerLength + Int(blobHeader.datasize))
            }
            if let idx = nodes.firstIndex(where: { references[$0.id] == nil }) {
                nodes.remove(at: idx)
            }
//            nodes = nodes.filter({ references[$0.id] != nil })
            print("File opened")
            
        } catch {
            print(error.localizedDescription)
            assert(false)
            return
        }

        readingGroup.wait() // Execution will stop here until the number of .enter() is balanced by the number of .leave() in the parallel closures
        let after = Date().timeIntervalSince1970
        print("Read in \(after - before) seconds")
    }
    
    func parseFile() {
        print("\nParsing the file")
        let primitiveBlocksCount = primitiveBLocks.count
        let entriesPerTask = primitiveBlocksCount / ProcessInfo.processInfo.processorCount
        let tasks = primitiveBlocksCount / entriesPerTask
        print("References: \(primitiveBlocksCount)")
        print("Tasks: \(tasks)")

        let before = Date().timeIntervalSince1970

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
                
                for primitiveBlock in primitiveBlocksForIteration { // MARK: ADD needed things
                    for primitiveGroup in primitiveBlock.primitivegroup { // MARK: Care when using and synchronizing the threads
                        let nodesReturned = handleNodes(primitiveGroup: primitiveGroup, latOffset: primitiveBlock.latOffset, lonOffset: primitiveBlock.lonOffset, granularity: primitiveBlock.granularity, stringTable: primitiveBlock.stringtable)
                        nodesToAdd.append(contentsOf: nodesReturned)
                        let arrayDictAppender = handleWays(primitiveGroup: primitiveGroup, stringTable: primitiveBlock.stringtable)
                        waysToAdd.append(contentsOf: arrayDictAppender.waysForAppending)
                        dictToAdd.merge(arrayDictAppender.dictForAppending) { $0 + $1 }
                    }
                }
                self.serialSyncQueue.sync { // MARK: Can we use here DispatchGroup, because it takes some time
                    self.nodes.append(contentsOf: nodesToAdd)
                    self.ways.append(contentsOf: waysToAdd)
                    self.references.merge(dictToAdd) { $0 + $1 }
                }
            }
        }
        print("\(nodes.count) - \(ways.count) - \(references.count)")
        let after = Date().timeIntervalSince1970
        print("Parsed in \(after - before) seconds")
    }
    
    func handleNodes(primitiveGroup: OSMPBF_PrimitiveGroup, latOffset: Int64, lonOffset: Int64, granularity: Int32, stringTable: OSMPBF_StringTable) -> [DenseNodeNew] { // TODO: See if I need key and values of Nodes
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
        return handledNodes
    }
    
    func handleWays(primitiveGroup: OSMPBF_PrimitiveGroup, stringTable: OSMPBF_StringTable) ->  ArrayDictAppender {
        var handledWays = [WayNew]()
        var handledDictionary = [Int: [WayNew]]()
        
        for way in primitiveGroup.ways {
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
            
            guard shouldAppend else { continue }
            
            var deltaDecoderID = DeltaDecoder(previous: 0)
            var nodeRefs = [Int]()
            for ref in way.refs {
                deltaDecoderID.previous = deltaDecoderID.previous + Int(ref)
                nodeRefs.append(deltaDecoderID.previous)
            }
            
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
        return ArrayDictAppender(ways: handledWays, dict: handledDictionary)
    }
    
    func cleanPrimitiveBlocks() {
        primitiveBLocks.removeAll()
    }
    
    func reduceMap() {
        print("\nReducing nodes & ways") // FIXME: what if road has turns but its the same way and node represent the turn, or how are we going to navigate if we do not have the distance or way is not finishing in the node
        
        let referencesCount = references.keys.count
        let entriesPerTask = referencesCount / ProcessInfo.processInfo.processorCount
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
                
                self.serialSyncQueue.sync { // MARK: Is that correct, .sync? | DispatchGroup | We need for sure to be .sync or maybe we could use DispatchGroup
                    nodeIdsToStay.append(contentsOf: currentIterrationToStay)
                }
            }
        }
        let after = Date().timeIntervalSince1970
        print("Reduced in \((after - before)) seconds")
        // TODO: remove nodes from nodeIdsToRemove | Maybe use threads | Can we run this in the background | I don't think we can use threads here
        print("Nodes which we want: \(nodeIdsToStay.count)")
        
        let start = Date().timeIntervalSince1970
        var newReferences = [Int: [WayNew]]()
        print("Start filtering dictionary")
        for nodeId in nodeIdsToStay {
            newReferences[nodeId] = references[nodeId]
        }
        references = newReferences
        let end = Date().timeIntervalSince1970
        print("Filtered dictionary in \(end - start) seconds")

//        for (crossroadNodeId, crossroadWays) in references {
//            for way in crossroadWays {
//                for nodeRef in way.nodeRefs {
//
//                }
//            }
//        }
        
        for (nodeId, ways) in references {
            
        }
        
        print("Done deleting not needed nodes")
    }
    
    func launch() {
        self.readFile()
        self.parseFile()
        self.cleanPrimitiveBlocks()
        self.reduceMap()
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
