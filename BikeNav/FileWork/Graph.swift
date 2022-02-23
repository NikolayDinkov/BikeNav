//
//  Graph.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 22.02.22.
//

import Foundation

struct Edge {
    let nodeStart: DenseNodeNew
    let nodeEnd: DenseNodeNew
    let weight: Double
    
    init(pair: PairDistance) {
        self.nodeStart = pair.startNode
        self.nodeEnd = pair.endNode
        self.weight = pair.distanceToPrevious
    }
}

struct Graph {
    let map: [Int: [Edge]]
    
    
    func findRoad(to nodeId: Int) {
        
    }
}

extension Edge: Codable {
    
}

extension Graph: Codable {
    
}
