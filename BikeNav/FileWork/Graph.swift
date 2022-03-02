//
//  Graph.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 22.02.22.
//

import Foundation
import SwiftUI

typealias ShortestWay = (nodes: [DenseNodeNew], distance: Double)

class PathSegment {
    var node: DenseNodeNew
    var distance: Double
    var segmentPrev: PathSegment?
    
    init(node: DenseNodeNew, distance: Double, segmentPrev: PathSegment?) {
        self.node = node
        self.distance = distance
        self.segmentPrev = segmentPrev
    }
    
}

struct Edge {
    let nodeEnd: DenseNodeNew
    let weight: Double
    
    init(pair: PairDistance) {
        self.nodeEnd = pair.endNode
        self.weight = pair.distanceToPrevious
    }
}

struct Graph {
    let map: [DenseNodeNew: [Edge]]
    
    func findRoad(from nodeIdStart: Int, to nodeIdEnd: Int) -> PathSegment {
        guard let nodeStart = map.keys.first(where: { $0.id == nodeIdStart }) else {
            return PathSegment(node: DenseNodeNew(id: 0, latCalculated: 0.0, lonCalculated: 0.0), distance: 0.0, segmentPrev: nil)
        }
        for edge in map[nodeStart]! {
            print(edge)
        }
        
        guard let nodeEnd = map.keys.first(where: { $0.id == nodeIdEnd }) else {
            return PathSegment(node: DenseNodeNew(id: 0, latCalculated: 0.0, lonCalculated: 0.0), distance: 0.0, segmentPrev: nil)
        }
        print("Starting node:  \(nodeStart)")
        var prioQueue = [PathSegment]() {
            didSet {
                prioQueue = prioQueue.sorted(by: { $0.distance < $1.distance } )
            }
        }
        prioQueue.append(PathSegment(node: nodeStart, distance: 0.0, segmentPrev: nil))
        print(prioQueue[0].node)
        while prioQueue.isEmpty == false {
            let pathCurrent = prioQueue.removeFirst()
            if pathCurrent.node == nodeEnd {
                return pathCurrent
            }
            print("\(pathCurrent.node) with distance: \(pathCurrent.distance) and nodes: ")
            
            for edge in map[pathCurrent.node]! {
                prioQueue.append(PathSegment(node: edge.nodeEnd, distance: pathCurrent.distance + edge.weight, segmentPrev: pathCurrent.segmentPrev))
            }
        }
        return PathSegment(node: nodeStart, distance: 0.0, segmentPrev: nil)
    }
    
    private func checkNext(nodeStart: DenseNodeNew, prioQueue: [PathSegment]) {

    }
}

extension Edge: Codable {
    
}

extension Graph: Codable {
    
}
