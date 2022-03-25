////
////  MapBoxView.swift
////  BikeNav
////
////  Created by Nikolay Dinkov on 28.02.22.
////
//
//import SwiftUI
//import MapboxMaps
//
//struct MapBoxViewVersion1: UIViewRepresentable {
//    private var graph: Graph
//    private var map: MapView
//
//    private var lineAnnotation: PolylineAnnotationManager
//    private var pointAnnotation: PointAnnotationManager
//
//    private var nodesSofia: [DenseNodeNew]
//
//    init() {
//        RawFile().launch(completionHandler: { graphNew in
//            self.graph = graphNew
//            let myResourceOptions = ResourceOptions(accessToken: Secrets.mapboxPublicToken)
//            let myMapInitOptions = MapInitOptions(resourceOptions: myResourceOptions, styleURI: StyleURI(rawValue: Secrets.mapboxStylePath))
//            map = MapView(frame: CGRect(x: 0, y: 0, width: 64, height: 64), mapInitOptions: myMapInitOptions)
//            map.ornaments.options.scaleBar.visibility = .hidden
//
//            lineAnnotation = map.annotations.makePolylineAnnotationManager(
//                id: "line_manager",
//                layerPosition: LayerPosition.default
//            )
//
//            pointAnnotation = map.annotations.makePointAnnotationManager(
//                id: "point_manager",
//                layerPosition: LayerPosition.above("line_manager")
//            )
//
//            nodesSofia = graph.map.keys.filter { node in
//                return node.latitude < (42.69 + 0.2) && node.latitude > (42.69 - 0.2) &&
//                node.longitude < (23.32 + 0.2) && node.longitude > (23.32 - 0.2)
//            }
//
//            pointAnnotation.annotations = graph.map.keys.map { node in
//                var annotation = PointAnnotation(coordinate: CLLocationCoordinate2D(latitude: node.latitude, longitude: node.longitude))
//                annotation.iconSize = 0.06
//                annotation.image = .init(image: UIImage(named: "reddot")!, name: "reddot")
//                return annotation
//            }
//
//            let sw = CLLocationCoordinate2D(latitude: 41.226810000,
//                                            longitude: 29.188190000)
//            let ne = CLLocationCoordinate2D(latitude: 44.217770000,
//                                            longitude: 22.348750000)
//
//            let shownRectBounds = CoordinateBounds(southwest: sw, northeast: ne)
//            let newCamera = map.mapboxMap.camera(for: shownRectBounds, padding: .zero, bearing: 0, pitch: 0)
//            map.mapboxMap.setCamera(to: newCamera)
//        })
//    }
//    
//    func makeUIView(context: Context) -> MapView {
//        let lines = graph.map.keys.flatMap { node -> [PolylineAnnotation] in
//            let edges = graph.map[node]!
//            return edges.map { edge -> PolylineAnnotation in
//                var annotation = PolylineAnnotation(
//                    lineCoordinates: [
//                        CLLocationCoordinate2D(latitude: node.latitude, longitude: node.longitude),
//                        CLLocationCoordinate2D(latitude: edge.nodeEnd.latitude, longitude: edge.nodeEnd.longitude)
//                    ]
//                )
//                
//                annotation.lineColor = .init(.green)
//                annotation.lineWidth = 4
//                return annotation
//            }
//            
//        }
//        
//        lineAnnotation.annotations = lines
//        
//        return map
//    }
//    
//    func updateUIView(_ uiView: MapView, context: Context) { }
//}
