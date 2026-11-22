import { ExtractLoadService, kindFromFeatures, kindFromFileName } from "../../src/service/extract-load-service";
import { PassThrough } from 'stream';

jest.mock('unzipper', () => ({ Parse: jest.fn() }));

const feature = (type: string, properties: Record<string, any>) => ({ type: 'Feature', geometry: { type, coordinates: [] }, properties });

describe('kindFromFileName', () => {
    it.each([
        ['nodes.geojson', 'nodes'],
        ['wsdotdataedmonds3.graph.edges.geojson', 'edges'],
        ['wsdotdataedmonds3_nodes.graph.edges.OSW.geojson', 'edges'],
        ['X.NODES.GEOJSON', 'nodes'],
        ['Edges.GeoJSON', 'edges'],
        ['zones.osw.geojson', 'zones'],
        ['x.points.OSW.geojson', 'points'],
        ['x.lines.osw.geojson', 'lines'],
        ['polygons.OSW.geojson', 'polygons'],
        ['my dataset.nodes.geojson', 'nodes'],
        ['osw.nodes_v2.geojson', 'extension'],
        ['supernodes.geojson', 'extension'],
        ['custom.geojson', 'extension'],
    ])('%s -> %s', (name, kind) => expect(kindFromFileName(name)).toBe(kind));
});

describe('kindFromFeatures', () => {
    it.each([
        ['edges', [feature('LineString', { _id: 'e1', _u_id: 'n1', _v_id: 'n2', highway: 'footway' })]],
        ['zones', [feature('Polygon', { _id: 'z1', _w_id: ['n1', 'n2', 'n1'] })]],
        ['nodes', [feature('Point', { _id: 'n1' }), feature('Point', { _id: 'n2', barrier: 'kerb', kerb: 'lowered' })]],
        ['points', [feature('Point', { _id: 'p1', amenity: 'bench' })]],
        ['points', [feature('Point', { _id: 'p1', barrier: 'bollard' })]],
        [undefined, [feature('Point', { _id: 'n1' })]],
        [undefined, [feature('LineString', { _id: 'l1', barrier: 'fence' })]],
        [undefined, [feature('Polygon', { _id: 'b1', building: 'yes' })]],
        [undefined, []],
    ])('%s', (kind, features) => expect(kindFromFeatures(features)).toBe(kind));
});

describe('processGeoJsonZipEntry routing', () => {
    const edge = feature('LineString', { _id: 'e1', _u_id: 'n1', _v_id: 'n2', highway: 'footway' });
    const zone = feature('Polygon', { _id: 'z1', _w_id: ['n1', 'n2', 'n1'], highway: 'pedestrian' });
    const kerb = feature('Point', { _id: 'n2', barrier: 'kerb' });
    const bare = feature('Point', { _id: 'n1' });
    const bench = feature('Point', { _id: 'p1', amenity: 'bench' });
    const fence = feature('LineString', { _id: 'l1', barrier: 'fence' });
    const building = feature('Polygon', { _id: 'b1', building: 'yes' });

    it.each([
        // Names the validation library accepts
        ['wsdotdataedmonds3_nodes.graph.edges.OSW.geojson', [edge], 'bulkInsertEdges'],
        ['wsdotdataedmonds3_nodes.graph.nodes.OSW.geojson', [bare, kerb], 'bulkInsertNodes'],
        ['x_nodes.graph.nodes.OSW.geojson', [bare], 'bulkInsertNodes'],
        ['x.points.OSW.geojson', [bench], 'bulkInsertPoints'],
        ['Edges.GeoJSON', [edge], 'bulkInsertEdges'],
        ['X.NODES.GEOJSON', [bare], 'bulkInsertNodes'],
        ['wsdotdataedmonds3_nodes.osw/wsdotdataedmonds3.graph.edges.geojson', [edge], 'bulkInsertEdges'],
        // The features settle the kind, whatever the name
        ['whatever.geojson', [edge], 'bulkInsertEdges'],
        ['whatever.geojson', [zone], 'bulkInsertZones'],
        ['whatever.geojson', [bare, kerb], 'bulkInsertNodes'],
        ['x.nodes.geojson', [edge], 'bulkInsertEdges'],
        ['osw.nodes_v2.geojson', [bare, kerb], 'bulkInsertNodes'],
        ['supernodes.geojson', [bench], 'bulkInsertPoints'],
        // The features do not settle the kind, so the name does
        ['x.nodes.geojson', [bare], 'bulkInsertNodes'],
        ['x.points.geojson', [bare], 'bulkInsertPoints'],
        ['x.lines.geojson', [fence], 'bulkInsertLines'],
        ['x.polygons.geojson', [building], 'bulkInsertPolygons'],
        ['x.edges.geojson', [], 'bulkInsertEdges'],
        // Neither the features nor the name settle the kind
        ['custom.geojson', [bare], 'bulkInsertExtension'],
        ['supernodes.geojson', [bare], 'bulkInsertExtension'],
        ['osw.nodes_v2.geojson', [bare], 'bulkInsertExtension'],
        ['whatever.geojson', [fence], 'bulkInsertExtension'],
    ])('%s holding %j -> %s', async (entryPath, features, expectedInsert) => {
        const service = new ExtractLoadService();
        const entry: any = new PassThrough();
        entry.type = 'File';
        entry.path = entryPath;

        jest.spyOn(console, 'log').mockImplementation(() => undefined);
        jest.spyOn(console, 'warn').mockImplementation(() => undefined);
        const meta = jest.spyOn(service, 'updateAdditionalFileData').mockResolvedValue(undefined);
        jest.spyOn(service, 'updateExtensionFileData').mockResolvedValue(1);
        const inserts = ['bulkInsertNodes', 'bulkInsertEdges', 'bulkInsertPoints', 'bulkInsertLines', 'bulkInsertPolygons', 'bulkInsertZones', 'bulkInsertExtension']
            .map((name) => [name, jest.spyOn(service as any, name).mockResolvedValue(undefined)] as const);

        setImmediate(() => entry.end(JSON.stringify({ type: 'FeatureCollection', features })));
        await (service as any).processGeoJsonZipEntry({} as any, entry, 'dataset123', 'user123');

        for (const [name, spy] of inserts) {
            expect(spy).toHaveBeenCalledTimes(name === expectedInsert ? 1 : 0);
        }
        expect(meta).toHaveBeenCalledTimes(expectedInsert === 'bulkInsertExtension' ? 0 : 1);
    });
});
