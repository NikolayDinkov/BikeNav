//
//  PBFExperiment.swift
//  GITProb
//
//  Created by Nikolay Dinkov on 16.12.21.
//
import Foundation
import SwiftProtobuf
import CoreLocation

typealias PartialDistance = (id: Int, distanceToPrevious: Double)
typealias PairDistance = (startNode: DenseNodeNew, endNode: DenseNodeNew, distanceToPrevious: Double)

//MARK: We use that there could be only one type in primitivegroups
class RawFile {
    private let serialSyncQueue = DispatchQueue(label: "serial.sync", qos: .userInteractive)
    private let parallelProcessingQueue = DispatchQueue(label: "parallel.dictionary", qos: .userInteractive, attributes: [.concurrent])
    
    private let fileManager: FileManager = .default

    private var primitiveBLocks: [OSMPBF_PrimitiveBlock] = [OSMPBF_PrimitiveBlock]()
    
    private var primitiveBLocksWithNodes: [OSMPBF_PrimitiveBlock] = [OSMPBF_PrimitiveBlock]()
    private var primitiveBLocksWithWays: [OSMPBF_PrimitiveBlock] = [OSMPBF_PrimitiveBlock]()
    
    private var nodes: [DenseNodeNew] = [DenseNodeNew]()
    private var ways: [WayNew] = [WayNew]()
    private var references: [Int: [WayNew]] = [Int: [WayNew]]() //MARK: It could be not int but DenseNodeNew and in it to be for example name later on for the graph and to delete nodes array once we calculate the distance of a way
        
    private func readFile() {
        print("Reading the file")
        guard let fileURL = Bundle.main.url(forResource: "bulgaria-220213.osm", withExtension: ".pbf") else {
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
            while parser.data.count > parser.offset { // MARK: Cannot use threads for the reading, but can for handling
                let headerLength = Int(parser.parseLEUInt32()!)
                let headerRange = parser.offset ..< (headerLength + parser.offset)
                let blobHeader = try OSMPBF_BlobHeader(serializedData: parser.data.subdata(in: headerRange))
                parser.offset += headerLength
                let blobRange = (parser.offset) ..< parser.offset + Int(blobHeader.datasize)
                let blobData = parser.data.subdata(in: blobRange)
                parser.offset += blobRange.count

                switch blobHeader.type {
                case "OSMHeader":
//                    let headerBlock = try OSMPBF_HeaderBlock(serializedData: decompressedData as Data)
                    break
                case "OSMData":
                    readingGroup.enter() // Start a task
                    parallelProcessingQueue.async {
                        do {
                            let blob = try OSMPBF_Blob(serializedData: blobData)
                            let compressedData = blob.zlibData.dropFirst(2) // blob.zlibData.subdata(in: 2..<blob.zlibData.count)
                            let decompressedData = try (compressedData as NSData).decompressed(using: .zlib)
                            let primitiveBlock = try OSMPBF_PrimitiveBlock(serializedData: decompressedData as Data)
                            self.serialSyncQueue.sync {
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
            }
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
    
    private func handlePrimitiveBlocks() {
        let before = Date().timeIntervalSince1970
        var forWays = false
        var forNode = false
        for primitiveBLock in primitiveBLocks {
            for primitiveGroup in primitiveBLock.primitivegroup {
                if primitiveGroup.hasDense == true {
                    forNode = true
                }
                if primitiveGroup.ways.count != 0 {
                    forWays = true
                }
            }
            if forNode == true {
                primitiveBLocksWithNodes.append(primitiveBLock)
            }
            if forWays == true {
                primitiveBLocksWithWays.append(primitiveBLock)
            }
        }
        primitiveBLocks.removeAll()
        let after = Date().timeIntervalSince1970
        print("Read in \(after - before) seconds")
        
    }
    
    private func parseWaysFromFile() {
        print("\nParsing the ways")
        let primitiveBlocksCount = primitiveBLocksWithWays.count
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
                let primitiveBlocksForIteration = primitiveBLocksWithWays[startIndex..<endIndex]
                
                var waysToAdd = [WayNew]()
                var dictToAdd = [Int: [WayNew]]()
                
                for primitiveBlock in primitiveBlocksForIteration { // MARK: ADD needed things
                    for primitiveGroup in primitiveBlock.primitivegroup { // MARK: Care when using and synchronizing the threads
                        let arrayDictAppender = handleWays(primitiveGroup: primitiveGroup, stringTable: primitiveBlock.stringtable)
                        waysToAdd.append(contentsOf: arrayDictAppender.waysForAppending)
                        dictToAdd.merge(arrayDictAppender.dictForAppending) { $0 + $1 }
                    }
                }
                self.serialSyncQueue.sync { // MARK: Can we use here DispatchGroup, because it takes some time
                    self.ways.append(contentsOf: waysToAdd)
                    self.references.merge(dictToAdd) { $0 + $1 }
                }
            }
        }
        primitiveBLocksWithWays.removeAll()
        
        let after = Date().timeIntervalSince1970
        print("Parsed in \(after - before) seconds")
        print("\(nodes.count) - \(ways.count) - \(references.count)")
        
    }
    
    private func parseNodes() {
        print("\nParsing the nodes")
        let primitiveBlocksCount = primitiveBLocksWithNodes.count
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
                let primitiveBlocksForIteration = primitiveBLocksWithNodes[startIndex..<endIndex]
                
                var nodesToAdd = [DenseNodeNew]()
                
                for primitiveBlock in primitiveBlocksForIteration { // MARK: ADD needed things
                    for primitiveGroup in primitiveBlock.primitivegroup { // MARK: Care when using and synchronizing the threads
                        let nodesReturned = handleNodes(primitiveGroup: primitiveGroup, latOffset: primitiveBlock.latOffset, lonOffset: primitiveBlock.lonOffset, granularity: primitiveBlock.granularity, stringTable: primitiveBlock.stringtable)
                            nodesToAdd.append(contentsOf: nodesReturned)
                    }
                }
                self.serialSyncQueue.sync { // MARK: Can we use here DispatchGroup, because it takes some time
                    self.nodes.append(contentsOf: nodesToAdd)
                }
            }
        }
        
        primitiveBLocksWithNodes.removeAll()
        
        let after = Date().timeIntervalSince1970
        print("Parsed in \(after - before) seconds")
        print("\(nodes.count) - \(ways.count) - \(references.count)")
        
    }
    
    private func handleNodes(primitiveGroup: OSMPBF_PrimitiveGroup, latOffset: Int64, lonOffset: Int64, granularity: Int32, stringTable: OSMPBF_StringTable) -> [DenseNodeNew] { // TODO: See if I need key and values of Nodes
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
            
            if references.keys.contains(deltaDecoderID.previous) {
                let newNode = DenseNodeNew(id: deltaDecoderID.previous,
                                           latCalculated: 0.000000001 * Double((Int(latOffset) + (Int(granularity) * deltaDecoderLat.previous))),
                                           lonCalculated: 0.000000001 * Double((Int(lonOffset) + (Int(granularity) * deltaDecoderLon.previous))))
                handledNodes.append(newNode)
            }
            
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
    
    private func handleWays(primitiveGroup: OSMPBF_PrimitiveGroup, stringTable: OSMPBF_StringTable) ->  ArrayDictAppender {
        var handledWays = [WayNew]()
        var handledDictionary = [Int: [WayNew]]()
        
        for way in primitiveGroup.ways {
            var shouldAppend = false
            var hasName = false
            var keyVal = [String: String]()
            for (key, value) in zip(way.keys, way.vals) {
                let keyString = String(data: stringTable.s[Int(key)], encoding: .utf8)!
                if keyString == "highway" {
                    shouldAppend = true
                } else if keyString == "name" {
                    hasName = true
                }
                let valString = String(data: stringTable.s[Int(value)], encoding: .utf8)!
                keyVal[keyString] = valString
            }
            
            guard shouldAppend, hasName else { continue }
            
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
    
    private func reduceMap() -> Graph {
        print("\nReducing nodes & ways") // FIXME: what if road has turns but its the same way and node represent the turn, or how are we going to navigate if we do not have the distance or way is not finishing in the node
        
        var referencesCount = references.keys.count
        var entriesPerTask = referencesCount / ProcessInfo.processInfo.processorCount
        var tasks = referencesCount / entriesPerTask
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
                    
                    let namedWays = ways.filter { $0.keyVal["name"] != nil }
                    let uniqueNames = Set(ways.compactMap { $0.keyVal["name"] })
                    if namedWays.count >= 3 || uniqueNames.count >= 2 { // MARK: ,ways.count > 1 is giving 7 less id's to add | threads not ok
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
        
        
        let before3 = Date().timeIntervalSince1970
        var referencesNew = [Int: [WayNew]]()
        for nodeId in nodeIdsToStay {
            referencesNew[nodeId] = references[nodeId]
        }
        let after3 = Date().timeIntervalSince1970
        print("Looped wanted nodes in \( after3 - before3 )")
        references = referencesNew
        
        let idTest = [543380730, 273346029, 1261063975, 277580507, 674039590, 253018846, 1699375901, 1261064074, 543380730, 1699375864, 6597459715]
        var newReferences = [Int: [WayNew]]()
        for nodeId in idTest {
            newReferences[nodeId] = references[nodeId]

        }
        references = newReferences

        print("Finding crossroad relations")
        
        referencesCount = references.keys.count
        entriesPerTask = referencesCount / ProcessInfo.processInfo.processorCount
        tasks = referencesCount / entriesPerTask
        
        var allEdges = [DenseNodeNew: [Edge]]()
        
        self.parallelProcessingQueue.sync {
            DispatchQueue.concurrentPerform(iterations: tasks) { offset in
                let startIndex = offset * entriesPerTask
                var endIndex: Int
                if offset == tasks - 1 {
                    endIndex = startIndex + (referencesCount - (entriesPerTask * tasks) + entriesPerTask)
                } else {
                    endIndex = startIndex + entriesPerTask // FIXME: Calculate the max final index | It's ok
                }
                var edgesToAppend = [DenseNodeNew: [Edge]]()
                let nodeIds = Array(self.references.keys)[startIndex..<endIndex]
                for nodeId in nodeIds {
//                    print("Started at \(nodeId)")
                    let ways = references[nodeId]
                    for way in ways! {
                        let pair = nextCrossroads(startCrossroadId: nodeId, way: way, nodeIdsToStay: nodeIdsToStay)
                        guard pair.isEmpty == false else {
                            continue
                        }
//                        for pair in pair {
//                            if pair.startNode.id != nodeId {
//                                print("1")
//                            }
//                        }
                        let edges = pair.map({ Edge(pair: $0) })
                        if edgesToAppend[pair.first!.startNode] != nil {
                            edgesToAppend[pair.first!.startNode]!.append(contentsOf: edges)
                        } else {
                            edgesToAppend[pair.first!.startNode] = edges
                        }
                    }
//                    print("Found relations for \(nodeId) in \(end - start) seconds")
                }
                self.serialSyncQueue.sync {
                    allEdges.merge(edgesToAppend) { $0 + $1 }
                }
            }
        }
        
        
        print("Done deleting not needed nodes")
        return Graph(map: allEdges)
    }

    // Either find the next nodeRef that is also a crossroad in references
    // Or if there is no next nodeRef - incomplete way - loop through self.ways to find the next segment where there is a crossroad
    // If no next crossroad is found in self.ways - the road starts not on a crossroad - the very first/last node must become a crossroad
    private func nextCrossroads(startCrossroadId: Int, way: WayNew, nodeIdsToStay: [Int]) -> [PairDistance] { // MARK: Maybe we can manage the no-names with their id's
        let currentNodeIndex = way.nodeRefs.firstIndex(of: startCrossroadId)!

        switch currentNodeIndex {
        case let x where x == 0:
            guard let crossroadNode = nodes.first(where: { $0.id == startCrossroadId }) else {
                print("Not good")
                return []
            }
            let partialDistance = inBeginning(way: way, crossroadNode: crossroadNode, nodeIdsToStay: nodeIdsToStay, startCrossroadId: startCrossroadId)
            return [(crossroadNode, nodes.first(where: {$0.id == partialDistance.id})!, partialDistance.distanceToPrevious)]
        case let x where x == way.nodeRefs.count - 1:
            guard let crossroadNode = nodes.first(where: { $0.id == startCrossroadId }) else {
                print("Not good")
                return []
            }
            let partialDistance = inEnd(way: way, crossroadNode: crossroadNode, nodeIdsToStay: nodeIdsToStay, startCrossroadId: startCrossroadId)
            return [(crossroadNode, nodes.first(where: {$0.id == partialDistance.id})!, partialDistance.distanceToPrevious)]
        default:
            guard let crossroadIndex = way.nodeRefs.firstIndex(where: { $0 == startCrossroadId }), let crossroadNode = nodes.first(where: { $0.id == startCrossroadId }) else {
                print("Not good")
                return []
            }
            let result = inMiddle(way: way, crossroadNode: crossroadNode, nodeIdsToStay: nodeIdsToStay, crossroadIndex: crossroadIndex)
            return [(crossroadNode, nodes.first(where: {$0.id == result.0.id})!, result.0.distanceToPrevious), (crossroadNode, nodes.first(where: {$0.id == result.1.id})!, result.1.distanceToPrevious)]//.compactMap { $0 }
        }
    }
    
    private func inBeginning(way: WayNew, crossroadNode: DenseNodeNew, nodeIdsToStay: [Int], startCrossroadId: Int) -> PartialDistance {
        var nextWayLength: Double = 0.0
        var previousLatNext: Double = crossroadNode.latitude
        var previousLonNext: Double = crossroadNode.longitude
        var nextCrossroadId = way.nodeRefs.first(where: { id in
            guard id != startCrossroadId, let crossroadNode = nodes.first(where: { $0.id == id }) else {
                return false
            }
            let location = CLLocation(latitude: crossroadNode.latitude, longitude: crossroadNode.longitude)
            let distance = location.distance(from: CLLocation(latitude: previousLatNext, longitude: previousLonNext))
            nextWayLength += distance
            previousLatNext = crossroadNode.latitude
            previousLonNext = crossroadNode.longitude
            return nodeIdsToStay.contains(id) // id != startCrossroadId &&
        })
        
        guard let nextCrossroadId = nextCrossroadId else {
            guard let nodeRef = way.nodeRefs.last, let referenceNodeNew = nodes.first(where: { $0.id == nodeRef}) else {
                assert(false)
                return (0, 0)
            }
            return firstCrossroad(referenceNode: referenceNodeNew, name: way.keyVal["name"]!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: nextWayLength, visitedWayIds: [way.id])//.compactMap { $0 } // MARK: change last/start , true/false
        }
        
        return (nextCrossroadId, nextWayLength)
    }
    
    private func inEnd(way: WayNew, crossroadNode: DenseNodeNew, nodeIdsToStay: [Int], startCrossroadId: Int) -> PartialDistance {
        var nextWayLength: Double = 0.0
        var previousLatNext: Double = crossroadNode.latitude
        var previousLonNext: Double = crossroadNode.longitude
        var nextCrossroadId = way.nodeRefs.last(where: { id in
            guard id != startCrossroadId, let crossroadNode = nodes.first(where: { $0.id == id }) else {
                return false
            }
            let location = CLLocation(latitude: crossroadNode.latitude, longitude: crossroadNode.longitude)
            let distance = location.distance(from: CLLocation(latitude: previousLatNext, longitude: previousLonNext))
            nextWayLength += distance
            previousLatNext = crossroadNode.latitude
            previousLonNext = crossroadNode.longitude
            return nodeIdsToStay.contains(id) // id != startCrossroadId &&
        })
        guard let nextCrossroadId = nextCrossroadId else {
            guard let nodeRef = way.nodeRefs.first, let referenceNodeNew = nodes.first(where: { $0.id == nodeRef}) else {
                assert(false)
                return (0, 0)
            }
            return firstCrossroad(referenceNode: referenceNodeNew, name: way.keyVal["name"]!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: nextWayLength, visitedWayIds: [way.id])//.compactMap { $0 }
        }
        return (nextCrossroadId, nextWayLength)
    }
    
    private func inMiddle(way: WayNew, crossroadNode: DenseNodeNew, nodeIdsToStay: [Int], crossroadIndex: Array<Int>.Index) -> (PartialDistance, PartialDistance) {
        var otherWayLength: Double = 0.0
        var nextWayLength: Double = 0.0
        
        let nodesPrev = way.nodeRefs[0 ..< crossroadIndex].reversed()
        var previousLatPrev: Double = crossroadNode.latitude
        var previousLonPrev: Double = crossroadNode.longitude
        let nodesNext = way.nodeRefs[(crossroadIndex + 1) ... (way.nodeRefs.count - 1)] //MARK: Is (index + 1) ok?
        var previousLatNext: Double = crossroadNode.latitude
        var previousLonNext: Double = crossroadNode.longitude
        
        var otherPartial: PartialDistance
        if let prevNodeId = nodesPrev.first(where: { id in
            guard let crossroadNode = nodes.first(where: { $0.id == id }) else {
                return false
            }
            let location = CLLocation(latitude: crossroadNode.latitude, longitude: crossroadNode.longitude)
            let distance = location.distance(from: CLLocation(latitude: previousLatPrev, longitude: previousLonPrev))
            otherWayLength += distance
            previousLatPrev = crossroadNode.latitude
            previousLonPrev = crossroadNode.longitude
            return nodeIdsToStay.contains(id)
            
        }) {
            otherPartial = (prevNodeId, otherWayLength)
        } else {
            guard let nodeRef = way.nodeRefs.first, let referenceNodeNew = nodes.first(where: { $0.id == nodeRef}) else {
                assert(false)
                return ((0, 0.0), (0, 0.0))
            }
            otherPartial = firstCrossroad(referenceNode: referenceNodeNew, name: way.keyVal["name"]!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: otherWayLength, visitedWayIds: [way.id])//.compactMap { $0 } // MARK: change last/start , true/false
        }
        
        var nextPartial: PartialDistance
        if let nextNodeId = nodesNext.first(where: { id in
            guard let crossroadNode = nodes.first(where: { $0.id == id }) else {
                return false
            }
            let location = CLLocation(latitude: crossroadNode.latitude, longitude: crossroadNode.longitude)
            let distance = location.distance(from: CLLocation(latitude: previousLatNext, longitude: previousLonNext))
            nextWayLength += distance
            previousLatNext = crossroadNode.latitude
            previousLonNext = crossroadNode.longitude
            return nodeIdsToStay.contains(id) // id != startCrossroadId &&
        }) {
            nextPartial = (nextNodeId, nextWayLength)
        } else {
            guard let nodeRef = way.nodeRefs.first, let referenceNodeNew = nodes.first(where: { $0.id == nodeRef}) else {
                assert(false)
                return ((0, 0.0), (0, 0.0))
            }
            nextPartial = firstCrossroad(referenceNode: referenceNodeNew, name: way.keyVal["name"]!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: nextWayLength, visitedWayIds: [way.id])//.compactMap { $0 }
        }
        
        return ((otherPartial.id, otherPartial.distanceToPrevious), (nextPartial.id, nextPartial.distanceToPrevious))
    }

    private func firstCrossroad(referenceNode: DenseNodeNew, name: String, referenceWayId: Int, nodeIdsToStay: [Int], length: Double, visitedWayIds: [Int]) -> PartialDistance {
        for way in ways {
            guard way.keyVal["name"] == name, way.id != referenceWayId, !visitedWayIds.contains(way.id) else {
                continue
            }
            var visitedWayIds = visitedWayIds
            visitedWayIds.append(referenceWayId)
            var previousLatNext = referenceNode.latitude
            var previousLonNext = referenceNode.longitude
            var nextWayLength = 0.0
            
            if way.nodeRefs.first == referenceNode.id {
                guard let crossroadId = way.nodeRefs.first(where: { id in
                    guard let crossroadNode = nodes.first(where: { $0.id == id }) else {
                        return false
                    }
                    let location = CLLocation(latitude: crossroadNode.latitude, longitude: crossroadNode.longitude)
                    let distance = location.distance(from: CLLocation(latitude: previousLatNext, longitude: previousLonNext))
                    nextWayLength += distance
                    previousLatNext = crossroadNode.latitude
                    previousLonNext = crossroadNode.longitude
                    return nodeIdsToStay.contains(id)
                }) else {
                    guard let nodeRef = way.nodeRefs.last, let referenceNodeNew = nodes.first(where: { $0.id == nodeRef}) else {
                        continue
                    }
                    return firstCrossroad(referenceNode: referenceNodeNew, name: way.keyVal["name"]!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: length, visitedWayIds: visitedWayIds)
                }
                return (crossroadId, length)
            } else if way.nodeRefs.last == referenceNode.id {
                guard let crossroadId = way.nodeRefs.last(where: { id in
                    guard let crossroadNode = nodes.first(where: { $0.id == id }) else {
                        return false
                    }
                    let location = CLLocation(latitude: crossroadNode.latitude, longitude: crossroadNode.longitude)
                    let distance = location.distance(from: CLLocation(latitude: previousLatNext, longitude: previousLonNext))
                    nextWayLength += distance
                    previousLatNext = crossroadNode.latitude
                    previousLonNext = crossroadNode.longitude
                    return nodeIdsToStay.contains(id)
                }) else {
                    guard let nodeRef = way.nodeRefs.first, let referenceNodeNew = nodes.first(where: { $0.id == nodeRef}) else {
                        continue
                    }
                    return firstCrossroad(referenceNode: referenceNodeNew, name: way.keyVal["name"]!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: length, visitedWayIds: visitedWayIds)
                }
                return (crossroadId, length)
            }
        }

        // FIXME if no other crossroad is found - find the very last (or first depending on start direction) node of the road
        return (referenceNode.id, length)
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

    var offset = 0

    init(data: Data) {
        self.data = data
    }
    
    private mutating func parseLEUIntX<Result>(_: Result.Type) -> Result?
    where Result: UnsignedInteger // May be Signed or UnSigned Integer if needed
    {
        let expected = MemoryLayout<Result>.size
        guard data.count >= offset + expected else { return nil }
        defer { offset += expected }
        
        
        return data[offset..<offset+expected].reduce(0, { soFar, new in (soFar << 8) | Result(new) })
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


extension RawFile {
    func launch() {
        let filePath = fileManager.urls(for: .documentDirectory, in: .userDomainMask)[0].appendingPathComponent("graph.short")
        print(filePath)
        if fileManager.fileExists(atPath: filePath.path) {
            do {
                let jsonData = try Data(contentsOf: filePath)
                let jsonDecoder = JSONDecoder()
                let graph = try jsonDecoder.decode(Graph.self, from: jsonData)
            } catch {
                print("Error opening the smaller file with the graph")
                return
            }
        } else {
            
            self.readFile()
            self.handlePrimitiveBlocks()
            self.parseWaysFromFile()
            self.parseNodes()
            do {
                let graph = self.reduceMap()
                let jsonResultData = try JSONEncoder().encode(graph)
                try jsonResultData.write(to: filePath)
            } catch {
                print("Error making smaller and faster for loading file")
                return
            }
            
        }
    }
}
