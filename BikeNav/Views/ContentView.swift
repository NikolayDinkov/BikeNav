//
//  ContentView.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 27.01.22.
//

import SwiftUI

struct ContentView: View {
    @State var graph: Graph?
    
    init() {
        RawFile().launch { myGraph in
            self.graph = myGraph
        }
    }
    
    var body: some View {
        if let graph = graph {
            MapboxMapView()
        } else {
            Text("Loading")
        }
        
    }
}

struct ContentView_Previews: PreviewProvider {
    static var previews: some View {
        ContentView()
    }
}
