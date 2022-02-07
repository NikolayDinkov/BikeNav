//
//  BikeNavApp.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 27.01.22.
//

import SwiftUI

@main
struct BikeNavApp: App {
    let rawFile = RawFile()
    
    var body: some Scene {
        WindowGroup {
            ContentView()
                .onAppear {
                    rawFile.openFile()
                    rawFile.reduceMap()
                }
        }
    }
}

