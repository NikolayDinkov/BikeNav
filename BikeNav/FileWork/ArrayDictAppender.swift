//
//  ArrayDictAppender.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 12.02.22.
//

import Foundation

struct ArrayDictAppender {
    let waysForAppending: [WayNew]
    let dictForAppending: [Int: [WayNew]]
    
    init(ways: [WayNew] , dict: [Int: [WayNew]]) {
        self.waysForAppending = ways
        self.dictForAppending = dict
    }
}
