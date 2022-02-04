//
//  WayNew.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 4.02.22.
//

import Foundation


struct WayNew {
    let id: Int
    let keyVal: [String: String]
    let nodeRefs: [Int]
    
    init(id: Int, keyVal: [String: String], nodeRefs: [Int]) {
        self.id = id
        self.keyVal = keyVal
        self.nodeRefs = nodeRefs
    }
}
