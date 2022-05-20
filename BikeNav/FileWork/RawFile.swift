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

class RawFile {
    private let serialSyncQueue = DispatchQueue(label: "serial.sync", qos: .userInteractive)
    private let parallelProcessingQueue = DispatchQueue(label: "parallel.dictionary", qos: .userInteractive, attributes: [.concurrent])
    
    private let fileManager: FileManager = .default

    private var primitiveBLocks: [OSMPBF_PrimitiveBlock] = [OSMPBF_PrimitiveBlock]()
    
    private var primitiveBLocksWithNodes: [OSMPBF_PrimitiveBlock] = [OSMPBF_PrimitiveBlock]()
    private var primitiveBLocksWithWays: [OSMPBF_PrimitiveBlock] = [OSMPBF_PrimitiveBlock]()
    
    private var nodes: [DenseNodeNew] = [DenseNodeNew]()
    private var ways: [WayNew] = [WayNew]()
    private var sortedWays: [WaySmaller] = [WaySmaller]()
    private var references: [Int: [WayNew]] = [Int: [WayNew]]() //MARK: It could be not int but DenseNodeNew and in it to be for example name later on for the graph and to delete nodes array once we calculate the distance of a way
        
    private func downloadFile(completionHandler: @escaping(Data?) -> Void) {
        print("Initiated downloading")
        let before = Date().timeIntervalSince1970
        
        let fileURL = URL(string: "https://download.geofabrik.de/europe/bulgaria-latest.osm.pbf")!
        
        URLSession.shared.dataTask(with: fileURL) { urlData, urlResponse, error in
            guard let urlData = urlData, error == nil else {
                completionHandler(nil)
                return
            }
            
            let fileData = urlData
            let after = Date().timeIntervalSince1970
            print("Downloaded in \(after - before) seconds")
            
            completionHandler(fileData)
        }.resume()
    }
    
    private func readFile(fileData: Data) {
        print("Reading the file")
        
//        guard let fileURL = Bundle.main.url(forResource: "bulgaria-latest.osm", withExtension: ".pbf") else {
//            assert(false)
//            return
//        }
        let before = Date().timeIntervalSince1970

        let readingGroup = DispatchGroup()
        do {
            var parser = Parser(data: fileData)
            while parser.data.count > parser.offset {
                let headerLength = Int(parser.parseLEUInt32()!)
                let headerRange = parser.offset ..< (headerLength + parser.offset)
                let blobHeader = try OSMPBF_BlobHeader(serializedData: parser.data.subdata(in: headerRange))
                parser.offset += headerLength
                let blobRange = (parser.offset) ..< parser.offset + Int(blobHeader.datasize)
                let blobData = parser.data.subdata(in: blobRange)
                parser.offset += blobRange.count

                switch blobHeader.type {
                case "OSMHeader":
                    break
                case "OSMData":
                    readingGroup.enter()
                    parallelProcessingQueue.async {
                        do {
                            let blob = try OSMPBF_Blob(serializedData: blobData)
                            let compressedData = blob.zlibData.dropFirst(2) // blob.zlibData.subdata(in: 2..<blob.zlibData.count)
                            let decompressedData = try (compressedData as NSData).decompressed(using: .zlib)
                            let primitiveBlock = try OSMPBF_PrimitiveBlock(serializedData: decompressedData as Data)
                            self.serialSyncQueue.sync {
                                self.primitiveBLocks.append(primitiveBlock)
                                readingGroup.leave()
                            }
                        } catch {
                            print(error.localizedDescription)
                            assert(false)
                            readingGroup.leave()
                        }
                    }
                default:
                    print("Not possible case")
                }
            }
            print("File opened")
        } catch {
            print(error.localizedDescription)
            assert(false)
            return
        }

        readingGroup.wait()
        let after = Date().timeIntervalSince1970
        print("Read in \(after - before) seconds")
    }
    
    private func handlePrimitiveBlocks() {
        let before = Date().timeIntervalSince1970
        for primitiveBLock in primitiveBLocks {
            var forWays = false
            var forNode = false
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
        print("Filtered in \(after - before) seconds")
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
                
                for primitiveBlock in primitiveBlocksForIteration {
                    for primitiveGroup in primitiveBlock.primitivegroup {
                        let arrayDictAppender = handleWays(primitiveGroup: primitiveGroup, stringTable: primitiveBlock.stringtable)
                        waysToAdd.append(contentsOf: arrayDictAppender.waysForAppending)
                        dictToAdd.merge(arrayDictAppender.dictForAppending) { $0 + $1 }
                    }
                }
                self.serialSyncQueue.sync {
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
                
                for primitiveBlock in primitiveBlocksForIteration {
                    for primitiveGroup in primitiveBlock.primitivegroup {
                        let nodesReturned = handleNodes(primitiveGroup: primitiveGroup, latOffset: primitiveBlock.latOffset, lonOffset: primitiveBlock.lonOffset, granularity: primitiveBlock.granularity, stringTable: primitiveBlock.stringtable)
                            nodesToAdd.append(contentsOf: nodesReturned)
                    }
                }
                self.serialSyncQueue.sync {
                    self.nodes.append(contentsOf: nodesToAdd)
                }
            }
        }
        
        primitiveBLocksWithNodes.removeAll()
        
        let after = Date().timeIntervalSince1970
        print("Parsed in \(after - before) seconds")
        print("\(nodes.count) - \(ways.count) - \(references.count)")
    }
    
    private func handleNodes(primitiveGroup: OSMPBF_PrimitiveGroup, latOffset: Int64, lonOffset: Int64, granularity: Int32, stringTable: OSMPBF_StringTable) -> [DenseNodeNew] {
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
        var deltaDecoderID = DeltaDecoder(previous: 0)
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
            var underConstruction = false
            var keyVal = [String: String]()
            for (key, value) in zip(way.keys, way.vals) {
                let keyString = String(data: stringTable.s[Int(key)], encoding: .utf8)!
                if keyString == "highway" {
                    shouldAppend = true
                } else if keyString == "name" || keyString == "ref" { // MARK: Here maybe we need to add somoething about "ref"
                    hasName = true
                } else if keyString == "construction" {
                    underConstruction = true
                    break
                }
                let valString = String(data: stringTable.s[Int(value)], encoding: .utf8)!
                keyVal[keyString] = valString
            }
            
            guard !underConstruction else { continue }
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
        print("\nReducing nodes & ways")
        
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
                    endIndex = startIndex + entriesPerTask
                }
                let nodeIds = Array(self.references.keys)[startIndex..<endIndex]
                
                var currentIterrationToStay: [Int] = []
                
                for id in nodeIds {
                    guard let ways = self.references[id] else {
                        print("Node with no ways")
                        continue
                    }
                    
                    let uniqueNames = Set(ways.compactMap { $0.keyVal["name"] ?? $0.keyVal["ref"] })
                    if (uniqueNames.count) >= 2 {
                        currentIterrationToStay.append(id)
                    }
                }
                
                self.serialSyncQueue.sync {
                    nodeIdsToStay.append(contentsOf: currentIterrationToStay)
                }
            }
        }
        
        let after = Date().timeIntervalSince1970
        print("Reduced in \((after - before)) seconds")
        print("Nodes which we want: \(nodeIdsToStay.count)")
        
        let before3 = Date().timeIntervalSince1970
        var referencesNew = [Int: [WayNew]]()
        for nodeId in nodeIdsToStay {
            referencesNew[nodeId] = references[nodeId]
        }
        let after3 = Date().timeIntervalSince1970
        print("Looped wanted nodes in \( after3 - before3 )")
        references = referencesNew
        
//        let idTest = [1699375901] // 4519762598, 3189953569, 543380730, 273346029, 1261063975, 277580507, 674039590, 253018846, 1699375901, 1261064074, 543380730, 1699375864, 6597459715, 728623370, 3189953570, 3189953570, 1699497995, 1699497961, 728623370
//        var newReferences = [Int: [WayNew]]()
//        for nodeId in idTest {
//            newReferences[nodeId] = references[nodeId]
//
//        }
//        references = newReferences

//        for way in ways {
//            guard let first = way.nodeRefs.first, let last = way.nodeRefs.last else {
//                fatalError()
//            }
//            if references[first] == nil {
//                references[first] = [way]
//            }
//            if references[last] == nil {
//                references[last] = [way]
//            }
//        }
        
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
                    endIndex = startIndex + entriesPerTask
                }
                var edgesToAppend = [DenseNodeNew: [Edge]]()
                let nodeIds = Array(self.references.keys)[startIndex..<endIndex]
                for nodeId in nodeIds {

                    let ways = references[nodeId]
                    for way in ways! {
                        let pairs = nextCrossroads(startCrossroadId: nodeId, way: way, nodeIdsToStay: nodeIdsToStay)
                        guard pairs.isEmpty == false else {
                            continue
                        }
//                        for pair in pair {
//                            if pair.startNode.id != nodeId {
//                                print("1")
//                            }
//                        }
//                        let edges = pair.map({ Edge(pair: $0) })
//                        if edgesToAppend[pair.first!.startNode] != nil {
//                            edgesToAppend[pair.first!.startNode]!.append(contentsOf: edges)
//                        } else {
//                            edgesToAppend[pair.first!.startNode] = edges
//                        }
                        for pair in pairs {
                            let edge = Edge(pair: pair)
                            if edgesToAppend[pair.startNode] != nil {
                                edgesToAppend[pair.startNode]!.append(edge)
                            } else {
                                edgesToAppend[pair.startNode] = [edge]
                            }
                        }
                    }
//                    print("Found relations for \(nodeId) in \(end - start) seconds")
                }
                for (start, edges) in edgesToAppend {
                    for edge in edges {
                        if references.keys.contains(edge.nodeEnd.id) == false {
                            edgesToAppend[edge.nodeEnd] = [Edge(pair: (startNode: edge.nodeEnd, endNode: start, distanceToPrevious: edge.weight))]
                        }
                    }
                }
                self.serialSyncQueue.sync {
                    allEdges.merge(edgesToAppend) { $0 + $1 }
                }
            }
        }
//        for (node, edges) in allEdges {
//            print("Node ID - \(node.id) with ")
//            for edge in edges {
//                print(edge.nodeEnd.id, edge.weight)
//            }
//            print()
//        }
//        print("Done deleting not needed nodes")
        return Graph(map: allEdges)
    }

    // Either find the next nodeRef that is also a crossroad in references
    // Or if there is no next nodeRef - incomplete way - loop through self.ways to find the next segment where there is a crossroad
    // If no next crossroad is found in self.ways - the road starts not on a crossroad - the very first/last node must become a crossroad
    private func nextCrossroads(startCrossroadId: Int, way: WayNew, nodeIdsToStay: [Int]) -> [PairDistance] {
        let currentNodeIndex = way.nodeRefs.firstIndex(of: startCrossroadId)!

        switch currentNodeIndex {
        case let x where x == 0:
            guard let crossroadNode = nodes.binarySearch(closure: {
                if $0.id < startCrossroadId { return .smaller }
                else if $0.id > startCrossroadId { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
                print("Not good")
                return []
            }
            let partialDistance = inBeginning(way: way, crossroadNode: crossroadNode, nodeIdsToStay: nodeIdsToStay, startCrossroadId: startCrossroadId)
            return [(crossroadNode, nodes.binarySearch(closure: {
                if $0.id < partialDistance.id { return .smaller }
                else if $0.id > partialDistance.id { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count)!, partialDistance.distanceToPrevious)]
        case let x where x == way.nodeRefs.count - 1:
            guard let crossroadNode = nodes.binarySearch(closure: {
                if $0.id < startCrossroadId { return .smaller }
                else if $0.id > startCrossroadId { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
                print("Not good")
                return []
            }
            let partialDistance = inEnd(way: way, crossroadNode: crossroadNode, nodeIdsToStay: nodeIdsToStay, startCrossroadId: startCrossroadId)
            return [(crossroadNode, nodes.binarySearch(closure: {
                if $0.id < partialDistance.id { return .smaller }
                else if $0.id > partialDistance.id { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count)!, partialDistance.distanceToPrevious)]
        default:
            guard let crossroadIndex = way.nodeRefs.firstIndex(where: { $0 == startCrossroadId }), let crossroadNode = nodes.binarySearch(closure: {
                if $0.id < startCrossroadId { return .smaller }
                else if $0.id > startCrossroadId { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
                print("Not good")
                return []
            }
            let result = inMiddle(way: way, crossroadNode: crossroadNode, nodeIdsToStay: nodeIdsToStay, crossroadIndex: crossroadIndex)
            return [(crossroadNode, nodes.binarySearch(closure: {
                if $0.id < result.0.id { return .smaller }
                else if $0.id > result.0.id { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count)!, result.0.distanceToPrevious), (crossroadNode, nodes.binarySearch(closure: {
                if $0.id < result.1.id { return .smaller }
                else if $0.id > result.1.id { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count)!, result.1.distanceToPrevious)]//.compactMap { $0 }
        }
    }
    
    private func inBeginning(way: WayNew, crossroadNode: DenseNodeNew, nodeIdsToStay: [Int], startCrossroadId: Int) -> PartialDistance {
        var nextWayLength: Double = 0.0
        var previousLatNext: Double = crossroadNode.latitude
        var previousLonNext: Double = crossroadNode.longitude
        var nextCrossroadId = way.nodeRefs.first(where: { id in
            guard id != startCrossroadId, let crossroadNode = nodes.binarySearch(closure: {
                if $0.id < id { return .smaller }
                else if $0.id > id { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
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
            guard let nodeRef = way.nodeRefs.last, let referenceNodeNew = nodes.binarySearch(closure: {
                if $0.id < nodeRef { return .smaller }
                else if $0.id > nodeRef { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
                assert(false)
                return (0, 0)
            }
            return firstCrossroad(referenceNode: referenceNodeNew, name: (way.keyVal["name"] ?? way.keyVal["ref"])!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: nextWayLength, visitedWayIds: [way.id])//.compactMap { $0 } // MARK: change last/start , true/false
        }
        
        return (nextCrossroadId, nextWayLength)
    }
    
    private func inEnd(way: WayNew, crossroadNode: DenseNodeNew, nodeIdsToStay: [Int], startCrossroadId: Int) -> PartialDistance {
        var nextWayLength: Double = 0.0
        var previousLatNext: Double = crossroadNode.latitude
        var previousLonNext: Double = crossroadNode.longitude
        var nextCrossroadId = way.nodeRefs.last(where: { id in
            guard id != startCrossroadId, let crossroadNode = nodes.binarySearch(closure: {
                if $0.id < id { return .smaller }
                else if $0.id > id { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
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
            guard let nodeRef = way.nodeRefs.first, let referenceNodeNew = nodes.binarySearch(closure: {
                if $0.id < nodeRef { return .smaller }
                else if $0.id > nodeRef { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
                assert(false)
                return (0, 0)
            }
            return firstCrossroad(referenceNode: referenceNodeNew, name: (way.keyVal["name"] ?? way.keyVal["ref"])!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: nextWayLength, visitedWayIds: [way.id])//.compactMap { $0 }
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
            guard let crossroadNode = nodes.binarySearch(closure: {
                if $0.id < id { return .smaller }
                else if $0.id > id { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
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
            guard let nodeRef = way.nodeRefs.first, let referenceNodeNew = nodes.binarySearch(closure: {
                if $0.id < nodeRef { return .smaller }
                else if $0.id > nodeRef { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
                assert(false)
                return ((0, 0.0), (0, 0.0))
            }
            otherPartial = firstCrossroad(referenceNode: referenceNodeNew, name: (way.keyVal["name"] ?? way.keyVal["ref"])!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: otherWayLength, visitedWayIds: [way.id])//.compactMap { $0 } // MARK: change last/start , true/false
        }
        
        var nextPartial: PartialDistance
        if let nextNodeId = nodesNext.first(where: { id in
            guard let crossroadNode = nodes.binarySearch(closure: {
                if $0.id < id { return .smaller }
                else if $0.id > id { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
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
            guard let nodeRef = way.nodeRefs.last, let referenceNodeNew = nodes.binarySearch(closure: {
                if $0.id < nodeRef { return .smaller }
                else if $0.id > nodeRef { return .bigger }
                else { return .even }
            }, range: 0..<nodes.count) else {
                assert(false)
                return ((0, 0.0), (0, 0.0))
            }
            nextPartial = firstCrossroad(referenceNode: referenceNodeNew, name: (way.keyVal["name"] ?? way.keyVal["ref"])!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: nextWayLength, visitedWayIds: [way.id])//.compactMap { $0 }
        }
        
        return ((otherPartial.id, otherPartial.distanceToPrevious), (nextPartial.id, nextPartial.distanceToPrevious))
    }

    private func firstCrossroad(referenceNode: DenseNodeNew, name: String, referenceWayId: Int, nodeIdsToStay: [Int], length: Double, visitedWayIds: [Int]) -> PartialDistance {
        for way in ways {
            guard way.keyVal["name"] == name || way.keyVal["ref"] == name else {
                continue
            }
            guard way.id != referenceWayId, !visitedWayIds.contains(way.id) else {
                continue
            }
            var visitedWayIds = visitedWayIds
            visitedWayIds.append(referenceWayId)
            var previousLatNext = referenceNode.latitude
            var previousLonNext = referenceNode.longitude
            var nextWayLength = length
            
            if way.nodeRefs.first == referenceNode.id {
                guard let crossroadId = way.nodeRefs.first(where: { id in
                    guard let crossroadNode = nodes.binarySearch(closure: {
                        if $0.id < id { return .smaller }
                        else if $0.id > id { return .bigger }
                        else { return .even }
                    }, range: 0..<nodes.count) else {
                        return false
                    }
                    let location = CLLocation(latitude: crossroadNode.latitude, longitude: crossroadNode.longitude)
                    let distance = location.distance(from: CLLocation(latitude: previousLatNext, longitude: previousLonNext))
                    nextWayLength += distance
                    previousLatNext = crossroadNode.latitude
                    previousLonNext = crossroadNode.longitude
                    return nodeIdsToStay.contains(id)
                }) else {
                    guard let nodeRef = way.nodeRefs.last, let referenceNodeNew = nodes.binarySearch(closure: {
                        if $0.id < nodeRef { return .smaller }
                        else if $0.id > nodeRef { return .bigger }
                        else { return .even }
                    }, range: 0..<nodes.count) else {
                        continue
                    }
                    return firstCrossroad(referenceNode: referenceNodeNew, name: (way.keyVal["name"] ?? way.keyVal["ref"])!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: nextWayLength, visitedWayIds: visitedWayIds)
                }
                return (crossroadId, nextWayLength)
            } else if way.nodeRefs.last == referenceNode.id {
                guard let crossroadId = way.nodeRefs.last(where: { id in
                    guard let crossroadNode = nodes.binarySearch(closure: {
                        if $0.id < id { return .smaller }
                        else if $0.id > id { return .bigger }
                        else { return .even }
                    }, range: 0..<nodes.count) else {
                        return false
                    }
                    let location = CLLocation(latitude: crossroadNode.latitude, longitude: crossroadNode.longitude)
                    let distance = location.distance(from: CLLocation(latitude: previousLatNext, longitude: previousLonNext))
                    nextWayLength += distance
                    previousLatNext = crossroadNode.latitude
                    previousLonNext = crossroadNode.longitude
                    return nodeIdsToStay.contains(id)
                }) else {
                    guard let nodeRef = way.nodeRefs.first, let referenceNodeNew = nodes.binarySearch(closure: {
                        if $0.id < nodeRef { return .smaller }
                        else if $0.id > nodeRef { return .bigger }
                        else { return .even }
                    }, range: 0..<nodes.count) else {
                        continue
                    }
                    return firstCrossroad(referenceNode: referenceNodeNew, name: (way.keyVal["name"] ?? way.keyVal["ref"])!, referenceWayId: way.id, nodeIdsToStay: nodeIdsToStay, length: nextWayLength, visitedWayIds: visitedWayIds)
                }
                return (crossroadId, nextWayLength)
            }
        }

        return (referenceNode.id, length)
    }
    
    private func sortNodesAndWays() {
        nodes.sort { $0.id < $1.id }
//        ways.sort { $0.id < $1.id }
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
        
        return data[offset..<offset + expected].reduce(0, { soFar, new in (soFar << 8) | Result(new) })
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

enum Comparison {
    case bigger
    case smaller
    case even
}

extension Collection where Iterator.Element: Comparable, Self.Index == Int {
    func binarySearch(closure: (Iterator.Element) -> Comparison, range: Range<Int>) -> Iterator.Element? {
        guard range.lowerBound < range.upperBound else {
            return nil
        }
        let index = range.lowerBound + (range.upperBound - range.lowerBound) / 2

        switch closure(self[index]) {
        case .even:
            return self[index]
        case .smaller:
            return binarySearch(closure: closure, range: (index + 1)..<range.upperBound)
        case .bigger:
            return binarySearch(closure: closure, range: range.lowerBound..<index)
        }
    }
    
}

extension RawFile {
    func launch(completionHandler: @escaping(Graph) -> Void) {
        let before = Date().timeIntervalSince1970
        let filePath = fileManager.urls(for: .documentDirectory, in: .userDomainMask)[0].appendingPathComponent("graph.short")
        print(filePath)
        if fileManager.fileExists(atPath: filePath.path) {
            do {
                let jsonData = try Data(contentsOf: filePath)
                let jsonDecoder = JSONDecoder()
                let graph = try jsonDecoder.decode(Graph.self, from: jsonData)
                let after = Date().timeIntervalSince1970
                print("Took \(after - before) seconds")
                completionHandler(graph)
            } catch {
                print("Error opening the smaller file with the graph")
                completionHandler(Graph(map: [:]))
            }
        } else {
            downloadFile { data in
                self.readFile(fileData: data!)
                self.handlePrimitiveBlocks()
                self.parseWaysFromFile()
                self.parseNodes()
                self.sortNodesAndWays()
                let graph = self.reduceMap()
                do {
                    let jsonResultData = try JSONEncoder().encode(graph)
                    try jsonResultData.write(to: filePath)
                } catch {
                    print("Error making smaller and faster for loading file")
                    completionHandler(Graph(map: [:]))
                }
                let after = Date().timeIntervalSince1970
                print("Took \(after - before) seconds")
                completionHandler(graph)
            }
        }
    }
}
