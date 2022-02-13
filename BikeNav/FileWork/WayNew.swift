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
    let nodeRefs: [Int] // MARK: Maybe I can use here the whole struct DenseNodeNew so when calculating the lenght it could be faster and also this may help removing the whole array in the RawFile file
    let lenght: Double
    
    init(id: Int, keyVal: [String: String], nodeRefs: [Int]) {
        self.id = id
        self.keyVal = keyVal
        self.nodeRefs = nodeRefs
        self.lenght = 0
    }
    
    func calculateLenght() {
        // MARK: Calc the lenght
        for nodeId in nodeRefs {
            
        }
    }
}

extension WayNew: Hashable {
    
}
