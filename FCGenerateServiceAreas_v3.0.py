import processing
import gc
from qgis.core import QgsProcessing
from qgis.core import QgsProcessingAlgorithm
from qgis.core import QgsProcessingParameterFeatureSource
from qgis.core import QgsProcessingMultiStepFeedback
from qgis.core import QgsProcessingParameterFeatureSink
from qgis.core import QgsProcessingParameterNumber
from qgis.core import QgsProcessingParameterString
from qgis.core import QgsExpression
from qgis.processing import alg
from qgis.core import QgsProject
from qgis.core import QgsVectorLayer
from qgis.core import edit
from qgis.core import QgsField
from PyQt5.QtCore import QVariant
from qgis.utils import iface
from qgis.core import QgsFeatureRequest




class FCServiceAreaV30(QgsProcessingAlgorithm):

    def initAlgorithm(self, config=None):

        self.addParameter(QgsProcessingParameterFeatureSource(name='MainRouteSketch', description='Route Sketch', optional=False, types=[QgsProcessing.TypeVectorAnyGeometry], defaultValue=None))
        self.addParameter(QgsProcessingParameterFeatureSource(name='RoadNetwork', description='Base Road Network', optional=False, types=[QgsProcessing.TypeVectorLine], defaultValue=None))
        self.addParameter(QgsProcessingParameterFeatureSink(name='ServiceAreas', description='ServiceAreas', optional=False, type=QgsProcessing.TypeVectorPolygon, createByDefault=True, defaultValue=None))
        self.addParameter(QgsProcessingParameterNumber(name='NumTiers', description='Number of Tiers', optional=False, type=QgsProcessingParameterNumber.Integer, defaultValue=6))
        self.addParameter(QgsProcessingParameterNumber(name='MilesPerTier', description='Driving Distance per Tier (mi)', optional=False, type=QgsProcessingParameterNumber.Double, defaultValue=2))
        self.addParameter(QgsProcessingParameterString(name='TierMinimums', description='Tier Minimums (as $ string, separated by | )', optional=True, defaultValue="$150|$200|$275|$350|$425|$500|$600|$700|$800"))
        self.addParameter(QgsProcessingParameterNumber(name='CostAvgSpeed', description='Average Driving Speed', optional=False, type=QgsProcessingParameterNumber.Integer, defaultValue=55))
        self.addParameter(QgsProcessingParameterNumber(name='CellSize', description='Service Area Cell Size (m)', optional=False, type=QgsProcessingParameterNumber.Integer, defaultValue=50))
        # self.addParameter(QgsProcessingParameterFeatureSink(name='ClipBuffer', description='ClipBuffer', optional=True, type=QgsProcessing.TypeVectorPolygon, createByDefault=False, defaultValue=None))
        # self.addParameter(QgsProcessingParameterFeatureSink(name='RoutePoints', description='RoutePoints', optional=True, type=QgsProcessing.TypeVectorPoint, createByDefault=False, defaultValue=None))
        # self.addParameter(QgsProcessingParameterFeatureSink(name='IsochroneRaw', description='IsochroneRaw', optional=True, type=QgsProcessing.TypeVectorPolygon, createByDefault=False, defaultValue=None))
        # self.addParameter(QgsProcessingParameterFeatureSink(name='SAGAIntersectRaw', description='SAGAIntersectRaw', optional=True, type=QgsProcessing.TypeVectorPolygon, createByDefault=False, defaultValue=None))


    def processAlgorithm(self, parameters, context, model_feedback):
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingMultiStepFeedback(6, model_feedback)
        results = {}
        outputs = {}
        vlayer_mainroutesketch = None
        vlayer_mainbufferarea = None
        vlayer_mainroadnetwork = None
        delimiter_tierminparam = '|'
        fid_sketchpointsuniqueid = 'UNIQUEID'
        fid_qneatisochrone_fid = 'fid'
        fid_qneatisochrone_costlevelid = 'id'
        fid_saga_selfintersectid = 'ID'
        fid_finalfid = 'fid'

        root = QgsProject().instance().layerTreeRoot()  
        lyrgroup = root.findGroup ('Results')
        if not lyrgroup:
            lyrgroup = root.insertGroup(0, 'Results')


        tier_count = parameters['NumTiers']
        tierid_max = tier_count - 1
        distcost_pertier_mi = parameters['MilesPerTier']
        distcost_pertier_m = self.convertMilesToMeters(distcost_pertier_mi)
        distcost_maxtier_mi = tier_count * distcost_pertier_mi
        distcost_maxtier_m = self.convertMilesToMeters(distcost_maxtier_mi)
        bufferdist_mi = distcost_maxtier_mi + (distcost_pertier_mi * 2)
        bufferdist_m = self.convertMilesToMeters(bufferdist_mi)

        tier_mins = parameters['TierMinimums'].split(delimiter_tierminparam)

        tier_specs = {}

        for tier_idx in range(tier_count):
            tier_num = tier_idx + 1
            entry = {}
            entry['tier_num'] = tier_num
            entry['tier_name'] = 'Tier %d' % tier_num if tier_num > 1 else 'Main Route'
            entry['travelcost_mi'] = distcost_pertier_mi * tier_num
            entry['travelcost_m'] = self.convertMilesToMeters(entry['travelcost_mi'])
            entry['order_minimum'] = self.getListItemWithDefault(tier_mins, tier_idx, d=None)
            tier_specs[tier_idx] = entry

        
        output_tablefields = {
            'tier_num': {'ftype': QVariant.Int, 'fname': 'TIERNUM'},
            'tier_name': {'ftype': QVariant.String, 'fname': 'TIERNAME'},
            'order_minimum': {'ftype': QVariant.String, 'fname': 'ORDERMIN'},
            'travelcost_mi': {'ftype': QVariant.Double, 'fname': '1WAYMILES'},
            'travelcost_m': {'ftype': QVariant.Double, 'fname': '1WAYMETERS'}
        }


        # routefields = {
        #     'RouteTier': {'ftype': QVariant.Int, 'infoid': 'tier_num'},
        #     'TierName': {'ftype': QVariant.String, 'infoid': 'tier_name'},
        #     'OrderMinimum': {'ftype': QVariant.String, 'infoid': 'minimum'},
        #     'OneWayMeters': {'ftype': QVariant.Int, 'infoid': 'travelcost_m'},
        #     'OneWayMiles': {'ftype': QVariant.Int, 'infoid': 'travelcost_mi'}
        # }

        for v in output_tablefields.values():
            v['qfieldobj'] = QgsField(v['fname'], v['ftype'])


        feedback.pushInfo(self.processAlgorithm.__name__ + ": Done with prep")
        feedback.pushInfo(self.processAlgorithm.__name__ + ": tier_specs = " + str(tier_specs))


        feedback.pushInfo(self.processAlgorithm.__name__ + ": Creating road network clipped by buffer %d miles" % bufferdist_mi)

        # Calculate buffer area around full route sketch
        vlayer_mainbufferarea = self.generateBufferAroundLayer(
            parameters,
            context,
            feedback,
            bufferdist_m,
            parameters['MainRouteSketch']
            )

        # Create base road network by clipping to main buffer
        alg_params = {
            'INPUT': parameters['RoadNetwork'],
            'OVERLAY': vlayer_mainbufferarea,
             'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            #'OUTPUT': parameters['RoadNetworkClipped']
            #'OUTPUT': parameters['DEBUGRoadsClipped'] if 'DEBUGRoadsClipped' in parameters else QgsProcessing.TEMPORARY_OUTPUT
        }
        vlayer_mainroadnetwork = processing.run(
            'native:clip', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']


        """
        Simplify route sketch to speed up processing
        Process:
         - Merge lines in road network
         - Multipart to single part (road network)
         - Line intersections (road network roads X roads)
         - Create circular buffer around intersection points, 20m
         - Extract by location / intersection (buffers X route sketch)
         - Get centroids of extracted
        """
        feedback.pushInfo(self.processAlgorithm.__name__ + ": Simplifying route to unique points @ road intersections")

        # Merge lines
        alg_params = {
            'INPUT': vlayer_mainroadnetwork,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        vlayer_temp1 = processing.run(
            'native:mergelines', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']

        # Multipart to singleparts
        alg_params = {
            'INPUT': vlayer_temp1,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        vlayer_temp1 = processing.run(
            'native:multiparttosingleparts', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']

        # Line intersections
        alg_params = {
            'INPUT': vlayer_temp1,
            'INPUT_FIELDS': [''],
            'INTERSECT': vlayer_temp1,
            'INTERSECT_FIELDS': [''],
            'INTERSECT_FIELDS_PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        vlayer_temp1 = processing.run(
            'native:lineintersections', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']

        # Buffer
        alg_params = {
            'DISSOLVE': False,
            'DISTANCE': 50,
            'END_CAP_STYLE': 0,
            'INPUT': vlayer_temp1,
            'JOIN_STYLE': 0,
            'MITER_LIMIT': 2,
            'SEGMENTS': 5,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            #'OUTPUT': parameters['DEBUGRoadIntersectionBuffers'] if 'DEBUGRoadIntersectionBuffers' in parameters else QgsProcessing.TEMPORARY_OUTPUT
        }
        vlayer_temp1 = processing.run(
            'native:buffer', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']

        # Extract by location
        alg_params = {
            'INPUT': vlayer_temp1,
            'INTERSECT': parameters['MainRouteSketch'],
            'PREDICATE': [0],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        vlayer_temp1 = processing.run(
            'native:extractbylocation', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']

        # Centroids
        alg_params = {
            'ALL_PARTS': False,
            'INPUT': vlayer_temp1,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            #'OUTPUT': parameters['DEBUGSimplifiedRouteSketch'] if 'DEBUGSimplifiedRouteSketch' in parameters else QgsProcessing.TEMPORARY_OUTPUT
        }
        vlayer_temp1 = processing.run(
            'native:centroids', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']

        # Dissolve + simplify centroid points to minimize point count
        alg_params = {
            'FIELD': [],
            'INPUT': vlayer_temp1,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            #'OUTPUT': parameters['RouteSketchSimplified']
        }
        vlayer_temp1 = processing.run(
            'native:dissolve', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']

        # Convert to single point layer
        alg_params = {
            'INPUT': vlayer_temp1,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            #'OUTPUT': parameters['RouteSketchSimplified']

        }
        vlayer_temp1 = processing.run(
            'native:multiparttosingleparts', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']


        feedback.pushInfo(self.processAlgorithm.__name__ + ": Starting isochrone calculations (QNEAT3)")


        # Add autoincremental field
        alg_params = {
            'FIELD_NAME': fid_sketchpointsuniqueid,
            'GROUP_FIELDS': [''],
            'INPUT': vlayer_temp1,
            'SORT_ASCENDING': True,
            'SORT_EXPRESSION': '',
            'SORT_NULLS_FIRST': False,
            'START': 0,
            #'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            'OUTPUT': parameters['RoutePoints'] if 'RoutePoints' in parameters else QgsProcessing.TEMPORARY_OUTPUT
        }
        vlayer_mainroutesketch = processing.run(
            'native:addautoincrementalfield', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']

        #results['RouteSketchSimplified'] = vlayer_mainroutesketch

        feedback.pushInfo(self.processAlgorithm.__name__ + ": Route points feature count = " + str(self.getLayerFeatureCount(context, vlayer_mainroutesketch)))

        """
        # Delete duplicate geometries
        alg_params = {
            'INPUT': vlayer_mainroutesketch,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            }
        vlayer_temp1 = processing.run(
            'native:deleteduplicategeometries', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']
        feedback.pushInfo(self.processAlgorithm.__name__ + ": Route points feature count DUPS REMOVED = " + str(self.getLayerFeatureCount(context, vlayer_temp1)))
        """

        # Iso-Area as Polygons (from Layer)
        alg_params = {
            'CELL_SIZE': parameters['CellSize'],
            'DEFAULT_DIRECTION': 2,
            'DEFAULT_SPEED': parameters['CostAvgSpeed'],
            'DIRECTION_FIELD': '',
            'ENTRY_COST_CALCULATION_METHOD': 0,
            'ID_FIELD': fid_sketchpointsuniqueid,
            'INPUT': vlayer_mainroadnetwork,
            'INTERVAL': distcost_pertier_m,   
            'MAX_DIST': distcost_maxtier_m,  
            'SPEED_FIELD': '',
            'START_POINTS': vlayer_mainroutesketch,
            'STRATEGY': 0,
            'TOLERANCE': 0,
            'VALUE_BACKWARD': '',
            'VALUE_BOTH': '',
            'VALUE_FORWARD': '',
            'OUTPUT_INTERPOLATION': QgsProcessing.TEMPORARY_OUTPUT,
            #'OUTPUT_POLYGONS': QgsProcessing.TEMPORARY_OUTPUT
            'OUTPUT_POLYGONS': parameters['IsochroneRaw'] if 'IsochroneRaw' in parameters else QgsProcessing.TEMPORARY_OUTPUT
        }

        vlayer_isochrone = processing.run(
            'qneat3:isoareaaspolygonsfromlayer', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT_POLYGONS']


        # DO: Run SAGA Polygon Self-Intersection on result
        # Polygon self-intersection
        feedback.pushInfo(self.processAlgorithm.__name__ + ": Starting SAGA Self-Intersection")

        alg_params = {
            'ID': fid_qneatisochrone_costlevelid,       
            'POLYGONS': vlayer_isochrone,
            'INTERSECT': parameters['SAGAIntersectRaw'] if 'SAGAIntersectRawG' in parameters else QgsProcessing.TEMPORARY_OUTPUT
            # 'INTERSECT': QgsProcessing.TEMPORARY_OUTPUT
        }
        intersect_results = processing.run(
            'saga:polygonselfintersection', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )
        vlayer_temp1 = intersect_results['INTERSECT']


        layer = QgsVectorLayer(vlayer_temp1, 'isochrone_selfintersection', 'ogr')


        # Add autoincremental field as new fid
        alg_params = {
            'FIELD_NAME': fid_finalfid,
            'GROUP_FIELDS': [''],
            'INPUT': layer,
            'SORT_ASCENDING': True,
            'SORT_EXPRESSION': '',
            'SORT_NULLS_FIRST': False,
            'START': 0,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            #'OUTPUT': parameters['ServiceAreas']
        }

        vlayer_temp1 = processing.run(
            'native:addautoincrementalfield', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']

        # DO: Clean up result
        #   - Add route fields to attribute table
        #   - Get the last (furthest-right) field -- this should be the ID added by SAGA to represent all the tiers this polygon corresponds to.
        #   - Create new field, populate with lowest route tier in each polygon
        #   - Add new incremented fid field
        feedback.pushInfo(self.processAlgorithm.__name__ + ": Starting self-intersection cleanup")

        #layer = QgsVectorLayer(vlayer_temp1, 'isochrone_intersected_incremented', 'ogr')
        layer = context.getMapLayer(vlayer_temp1)
        prov = layer.dataProvider()

        lyr_fieldnames_orig = list(prov.fieldNameMap())
        newfields_objs = [ val['qfieldobj'] for val in output_tablefields.values() ]
        newfields_keys = list(output_tablefields)

        with edit(layer):
            added = prov.addAttributes(newfields_objs)
            layer.updateFields()

        with edit(layer):
            # Populate new fields in each feature
            for f in layer.getFeatures():
                tier_idx = None
                intersect_idfieldval = f[fid_saga_selfintersectid]

                if intersect_idfieldval:
                    intersect_tiers = [ int(s) for s in intersect_idfieldval.split(delimiter_tierminparam) ]
                    tier_idx = min(intersect_tiers)

                    if tier_idx in tier_specs.keys():
                        for k in newfields_keys:
                            f[output_tablefields[k]['fname']] = tier_specs[tier_idx][k] 

                layer.updateFeature(f)
            layer.updateFields()

        with edit(layer):
            # Delete old fields
            lyr_fieldnamemap_final = prov.fieldNameMap() 
            lyr_fieldidxstodelete = [ lyr_fieldnamemap_final[n] for n in lyr_fieldnames_orig ]

            prov.deleteAttributes(lyr_fieldidxstodelete)
            layer.updateFields()

        # Dissolve final service area polygon
        alg_params = {
            'FIELD': [ output_tablefields['tier_num']['fname'] ],
            'INPUT': vlayer_temp1,
            #'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            'OUTPUT': parameters['ServiceAreas']
        }
        
        vlayer_final = processing.run(
            'native:dissolve', alg_params, context=context, feedback=feedback, is_child_algorithm=True
            )['OUTPUT']

        results['ServiceAreas'] = vlayer_final
        return results

    def getLayerAttrNames(
        self, layer
        ):
        return layer.dataProvider().fields().names()

    def convertMilesToMeters(
        self, miles
        ):
        return miles * 1609.344


    def getListItemWithDefault(
        self, l, i, d=None
        ):
        result = None
        try:
            result = l[i]
        except IndexError:
            result = d
        return result

    def printAllFeatures(
        self,
        parameters,
        context,
        model_feedback,
        layer_id=None,
        layer=None
        ):
        if not layer_id is None:
            layer = context.getMapLayer(layer_id)

        if not layer is None:
            for f in layer.getFeatures():
                f_fnames = f.fields().names()
                f_attrmap = dict([ (fname, f[fname]) for fname in f_fnames ])
                        
                model_feedback.pushInfo(
                    '______Feat (%d) attr map: %s' % (f.id(), str(f_attrmap))
                    )


    def generateBufferAroundLayer(
        self,
        parameters,
        context,
        model_feedback,
        buffer_distance,
        vlayer_base
        ):

        # Buffer
        alg_params = {
            'DISSOLVE': True,
            'DISTANCE': buffer_distance,
            'END_CAP_STYLE': 0,
            'INPUT': vlayer_base,
            'JOIN_STYLE': 0,
            'MITER_LIMIT': 2,
            'SEGMENTS': 5,
            #'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            'OUTPUT': parameters['ClipBuffer'] if 'ClipBuffer' in parameters else QgsProcessing.TEMPORARY_OUTPUT
        }
        return processing.run(
            'native:buffer', alg_params, context=context, feedback=model_feedback, is_child_algorithm=True
            )['OUTPUT']




    def getLayerFeatureCount(self, context, layer_id):
        return context.getMapLayer(layer_id).dataProvider().featureCount()
        
    def name(self):
        return 'FCServiceAreaV30'

    def displayName(self):
        return 'FCServiceAreaV30'

    def group(self):
        return ''

    def groupId(self):
        return ''

    def createInstance(self):
        return FCServiceAreaV30()

