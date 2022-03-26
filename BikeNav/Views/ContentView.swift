//
//  ContentView.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 27.01.22.
//

import SwiftUI

struct ContentView: View {
    @State var graph: Graph?
    
    var body: some View {
        if let graph = graph {
            MapboxMapView(myGraph: graph)
        } else {
            Text("Loading")
                .onAppear {
                    RawFile().launch { myGraph in
                        self.graph = myGraph
                    }
                }
        }
    }
}

struct ContentView_Previews: PreviewProvider {
    static var previews: some View {
        ContentView()
    }
}
