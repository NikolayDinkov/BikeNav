//
//  ContentView.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 27.01.22.
//

import SwiftUI

struct ContentView: View {
    @State var graph: Graph?
    @State private var isLoading = false
    
    var body: some View {
        if let graph = graph {
            ZStack {
                MapboxMapView(myGraph: graph, isLoading: $isLoading)

                if isLoading {
                    Color.black.opacity(0.1)
                    ProgressView()
                        .progressViewStyle(CircularProgressViewStyle(tint: .blue))
                        .scaleEffect(2.0)
                }
            }
        } else {
            VStack {
                ProgressView("Loading, please wait...")
                    .progressViewStyle(CircularProgressViewStyle(tint: .blue))
                    .scaleEffect(2.0)
                    .onAppear {
                        RawFile().launch { myGraph in
                            self.graph = myGraph
                        }
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
