//
//  ContentView.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 27.01.22.
//

import SwiftUI

struct ContentView: View {
    private var graph: Graph
    
    init(graph: Graph) {
        self.graph = graph
    }
    
    var body: some View {
        MapBoxViewVersion1(graph: graph)
    }
}

struct ContentView_Previews: PreviewProvider {
    static var previews: some View {
        ContentView(graph: Graph(map: [:]))
    }
}
