//
//  DenseNodeNew.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 4.02.22.
//

import Foundation


struct DenseNodeNew {
    let id: Int
    let latitude: Double //latitude = .000000001 * (lat_offset + (granularity * lat))
    let longitude: Double //longitude = .000000001 * (lon_offset + (granularity * lon))
    
    init(id: Int, latCalculated: Double, lonCalculated: Double) {
        self.id = id
        self.latitude = latCalculated
        self.longitude = lonCalculated
    }
}

extension DenseNodeNew: Equatable {
    
}
