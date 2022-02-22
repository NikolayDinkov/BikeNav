//
//  WayNew.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 4.02.22.
//

import Foundation
import CoreLocation


struct WayNew {
    let id: Int
    let keyVal: [String: String]
    let nodeRefs: [Int] // MARK: Maybe I can use here the whole struct DenseNodeNew so when calculating the lenght it could be faster and also this may help removing the whole array in the RawFile file
    var length: Double
    
    init(id: Int, keyVal: [String: String], nodeRefs: [Int]) {
        self.id = id
        self.keyVal = keyVal
        self.nodeRefs = nodeRefs
        self.length = 0.0
    }
    
    mutating func calculateLength(nodes: [DenseNodeNew]) {
        // MARK: Calc the lenght | Maybe we can use threads and maybe use only one variable when getting from nodes by id
        var previousLat: Double = 0.0
        var previousLon: Double = 0.0
        for nodeId in nodeRefs {
            guard let idx = nodes.firstIndex(where: { nodeId == $0.id }) else {
                print("No id of node which is bad")
                assert(false)
                continue
            }

            if previousLat == 0.0 && previousLon == 0.0 {
                previousLat = nodes[idx].latitude
                previousLon = nodes[idx].longitude
            } else {
                let location = CLLocation(latitude: nodes[idx].latitude, longitude: nodes[idx].longitude)
                let distance = location.distance(from: CLLocation(latitude: previousLat, longitude: previousLon))
                self.length = self.length + distance
            }
        }
    }
}

extension WayNew: Hashable {
    
}

struct WaySmaller {
    let id: Int
    let name: String
    let length: Double
    
    let nodeRefs: [Int]
    
    init(id: Int, name: String, length: Double, nodeRefs: [Int]) {
        self.id = id
        self.name = name
        self.length = length
        self.nodeRefs = nodeRefs
    }
}
