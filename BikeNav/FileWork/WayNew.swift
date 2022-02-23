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
