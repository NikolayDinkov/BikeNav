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
    let lenght: Double
    
    init(id: Int, keyVal: [String: String], nodeRefs: [Int]) {
        self.id = id
        self.keyVal = keyVal
        self.nodeRefs = nodeRefs
        self.lenght = 0
    }
    
    func calculateLenght() {
        // MARK: Calc the lenght
    }
}

extension WayNew: Hashable {
    
}
