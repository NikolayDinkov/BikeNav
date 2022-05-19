//
//  MapboxMapView.swift
//  BikeNav
//
//  Created by Nikolay Dinkov on 7.03.22.
//

import SwiftUI
import MapboxMaps

struct MapboxMapView: UIViewRepresentable {
    private var graph: Graph?
    
    private var map: MapView
    private var lineAnnotation: PolylineAnnotationManager
    private var pointAnnotation: PointAnnotationManager
    
    private var pointAnnotationManagerStart: PointAnnotationManager
    private var pointAnnotationManagerEnd: PointAnnotationManager
    
    private let nodesRoute: [DenseNodeNew] = []
    
    init(myGraph: Graph) {
        let myResourceOptions = ResourceOptions(accessToken: Secrets.mapboxPublicToken)
        let centerCoordinate = CLLocationCoordinate2D(latitude: 42.698334, longitude: 23.319941)
        let myMapInitOptions = MapInitOptions(resourceOptions: myResourceOptions, cameraOptions: CameraOptions(center: centerCoordinate, zoom: 11.0))
        map = MapView(frame: CGRect(x: 0, y: 0, width: 64, height: 64), mapInitOptions: myMapInitOptions)
        map.ornaments.options.scaleBar.visibility = .hidden

        lineAnnotation = map.annotations.makePolylineAnnotationManager(
            id: "line_manager",
            layerPosition: LayerPosition.default
        )
        
        pointAnnotation = map.annotations.makePointAnnotationManager(
            id: "point_manager",
            layerPosition: LayerPosition.above("line_manager")
        )

        pointAnnotationManagerStart = map.annotations.makePointAnnotationManager(
            id: "start_manager"
        )

        pointAnnotationManagerEnd = map.annotations.makePointAnnotationManager(
            id: "end_manager"
        )
        
        self.graph = myGraph
    }
    
    func makeUIView(context: Context) -> MapView {

        map.gestures.singleTapGestureRecognizer.addTarget(context.coordinator, action: #selector(Coordinator.handleMapTap(sender:)))
        return map
    }
    
    func makeCoordinator() -> Coordinator {
        if graph != nil {
            return Coordinator(graph: graph!)
        } else {
            return Coordinator(graph: Graph(map: [:]))
        }
    }
    
    func updateUIView(_ uiView: MapView, context: Context) { }
}

extension MapboxMapView {
    final class Coordinator: NSObject {
        private let graph: Graph
        private var start: CLLocationCoordinate2D?
        
        init(graph: Graph) {
            self.graph = graph
        }
        
        @objc func handleMapTap(sender: UITapGestureRecognizer) {
            guard let mapView = sender.view as? MapView else {
                assert(false)
                return
            }
            
            let point = sender.location(in: mapView)
            let coordinate = mapView.mapboxMap.coordinate(for: point)
            
            if start == nil {
                start = coordinate
                var startAnnotation = PointAnnotation(coordinate: start!)
                startAnnotation.image = .init(image: UIImage(named: "red_pin")!, name: "red_pin")
                (mapView.annotations.annotationManagersById["start_manager"] as! PointAnnotationManager).annotations = [startAnnotation]
            } else {
                var endAnnotation = PointAnnotation(coordinate: coordinate)
                endAnnotation.image = .init(image: UIImage(named: "blue_pin")!, name: "blue_pin")
                (mapView.annotations.annotationManagersById["end_manager"] as! PointAnnotationManager).annotations = [endAnnotation]
                
                findNprintRoute(start: start!, end: coordinate, map: mapView)
                start = nil
            }
        }
        
        private func findNprintRoute(start: CLLocationCoordinate2D, end: CLLocationCoordinate2D, map: MapView) {
            let before = Date().timeIntervalSince1970
            
            let array = Array(graph.map.keys)
            let startNode = array.sorted(by: {
                return abs($0.latitude - start.latitude) + abs($0.longitude - start.longitude) < abs($1.latitude - start.latitude) + abs($1.longitude - start.longitude)
            })[0]
            
            let endNode = array.sorted(by: {
                return abs($0.latitude - end.latitude) + abs($0.longitude - end.longitude) < abs($1.latitude - end.latitude) + abs($1.longitude - end.longitude)
            })[0]
            
            print(startNode)
            print(endNode)
            
            var route = graph.findRoad(from: startNode.id, to: endNode.id)
            var nodesOfRoute = [DenseNodeNew]()
            nodesOfRoute.append(route.node)
            while let prevSegment = route.segmentPrev {
                route = prevSegment
                nodesOfRoute.append(prevSegment.node)
            }
            
            var lines = [PolylineAnnotation]()
            var prevNode = nodesOfRoute[0]
            for node in nodesOfRoute[1 ..< nodesOfRoute.count] {
                var annotation = PolylineAnnotation(
                    lineCoordinates: [
                        CLLocationCoordinate2D(latitude: prevNode.latitude, longitude: prevNode.longitude),
                        CLLocationCoordinate2D(latitude: node.latitude, longitude: node.longitude)
                    ]
                )
                annotation.lineColor = .init(.green)
                annotation.lineWidth = 4
                lines.append(annotation)
                prevNode = node
            }
            
            let after = Date().timeIntervalSince1970
            print("It took \(after - before) seconds")
            
            (map.annotations.annotationManagersById["point_manager"] as! PointAnnotationManager).annotations = nodesOfRoute.map { node in
                var annotation = PointAnnotation(coordinate: CLLocationCoordinate2D(latitude: node.latitude, longitude: node.longitude))
                annotation.iconSize = 0.20
                annotation.image = .init(image: UIImage(named: "reddot")!, name: "reddot")
                return annotation
            }
            (map.annotations.annotationManagersById["line_manager"] as! PolylineAnnotationManager).annotations = lines
        }
    }
}
