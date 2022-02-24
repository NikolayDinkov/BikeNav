//
//  Graph.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 22.02.22.
//

import Foundation

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
    
    func findRoad(from nodeIdStart: Int, to nodeIdEnd: Int) {
        
    }
}

extension Edge: Codable {
    
}

extension Graph: Codable {
    
}
