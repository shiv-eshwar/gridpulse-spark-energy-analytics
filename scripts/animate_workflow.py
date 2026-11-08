"""Draw a schematic workflow GIF from the project's measured result artifacts.

Uses Pillow only. Run after sourcing scripts/env.sh. Animation timing is
illustrative and does not encode throughput or benchmark measurements.
"""
from pathlib import Path
import json
import math
import textwrap
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / 'docs/assets'
W, H = 1440, 900
C = dict(bg='#F3F7FA', ink='#16324F', muted='#526778', line='#C3D1DB',
         teal='#007F82', teal_light='#D7F0EE', orange='#D87829', white='#FFFFFF',
         green='#166952', pale='#EAF5F0')


def font(size, bold=False, mono=False):
    names = (["/System/Library/Fonts/Supplemental/Courier New.ttf",
              '/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf'] if mono else
             [f"/System/Library/Fonts/Supplemental/Arial{' Bold' if bold else ''}.ttf",
              f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf"])
    for name in names:
        if Path(name).exists():
            return ImageFont.truetype(name, size)
    return ImageFont.load_default(size=size)


F = {name: font(size, bold, mono) for name, size, bold, mono in [
    ('brand', 44, True, False), ('subtitle', 24, False, False),
    ('title', 25, True, False), ('body', 21, False, False),
    ('small', 18, False, False), ('tag', 17, True, False),
    ('phase', 29, True, False), ('detail', 24, False, False)]}


def point_on_path(points, progress):
    lengths = [math.dist(a, b) for a, b in zip(points, points[1:])]
    distance = progress * sum(lengths)
    for a, b, length in zip(points, points[1:], lengths):
        if distance <= length:
            t = distance / length
            return a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])
        distance -= length
    return points[-1]


def arrow(draw, points, active=False, done=False, progress=0):
    color = C['teal'] if active else ('#73B5B2' if done else C['line'])
    draw.line(points, fill=color, width=5 if active else 3, joint='curve')
    a, b = points[-2:]
    theta = math.atan2(b[1]-a[1], b[0]-a[0])
    length = 11
    draw.polygon([b, (b[0]-length*math.cos(theta-.5), b[1]-length*math.sin(theta-.5)),
                     (b[0]-length*math.cos(theta+.5), b[1]-length*math.sin(theta+.5))], fill=color)
    if active:
        for offset in [0, .34, .68]:
            x, y = point_on_path(points, (progress + offset) % .94)
            draw.ellipse((x-10, y-10, x+10, y+10), fill=C['teal_light'])
            draw.ellipse((x-5, y-5, x+5, y+5), fill=C['teal'])


def label(draw, xy, text):
    box = draw.textbbox((0, 0), text, font=F['small'])
    width = box[2]-box[0]
    x, y = xy
    draw.rounded_rectangle((x-8, y-4, x+width+8, y+24), 5, fill=C['bg'])
    draw.text((x, y), text, font=F['small'], fill=C['muted'])


def node(draw, xy, number, title, lines, active=False, done=False):
    x, y = xy
    fill = '#EEF9F7' if active else C['white']
    border = C['orange'] if active else ('#8FC4BD' if done else '#D0DCE4')
    draw.rounded_rectangle((x, y+4, x+300, y+154), 17, fill='#E5ECF1')
    draw.rounded_rectangle((x, y, x+300, y+150), 17, fill=fill, outline=border, width=3 if active else 2)
    draw.text((x+20, y+17), title, font=F['title'], fill=C['ink'])
    for i, line in enumerate(lines):
        assert draw.textlength(line, font=F['body']) <= 263, (title, line)
        draw.text((x+20, y+59+i*26), line, font=F['body'], fill=C['muted'])
    draw.ellipse((x+271, y-12, x+303, y+20), fill=C['orange'] if active else C['ink'])
    draw.text((x+287, y+4), str(number), font=F['tag'], fill=C['white'], anchor='mm')


def main():
    quality = json.loads((ROOT/'outputs/quality.json').read_text())
    validation = json.loads((ROOT/'outputs/streaming/validation.json').read_text())
    assert quality['rdd_sql_reconciliation'] == 'PASS'
    assert validation['batch_stream_parity'] == 'PASS'
    nodes = [
        ((60, 245), 'UCI telemetry', [f"{quality['source_rows']:,} source rows", 'Minute-level readings', 'Pinned SHA-256 + CRC']),
        ((400, 245), 'RDD processing', ['map / filter / flatMap', 'reduceByKey + actions', f"{quality['valid_rows']:,} valid readings"]),
        ((740, 245), 'DataFrames', ['Explicit typed schema', 'Clean + deduplicate', 'kWh + coverage checks']),
        ((1080, 245), 'SQL + calibration', ['Monthly + hourly views', 'Fit through 2008', 'Calibrate on 2009']),
        ((60, 555), 'File replay', [f"{validation['real_records_replayed']:,} real records", '6 atomic file arrivals', 'Historical-data replay']),
        ((400, 555), 'Event-time windows', ['Reject nonfinite input', 'Watermark + 1h groups', 'Count / mean / kWh']),
        ((740, 555), 'Coverage + scoring', ['54+ readings: score', 'Below 54: null alert', 'Static weekly profile']),
        ((1080, 555), 'Validate + retain', ['Checkpoint restart', f"{validation['hourly_windows']:,} windows match batch", 'Snapshots + evidence']),
    ]
    edges = [([(360,320),(400,320)],1), ([(700,320),(740,320)],2),
             ([(1040,320),(1080,320)],3),
             ([(890,395),(890,440),(210,440),(210,555)],4),
             ([(360,630),(400,630)],5), ([(700,630),(740,630)],6),
             ([(1230,395),(1230,480),(890,480),(890,555)],6),
             ([(1040,630),(1080,630)],7),
             ([(210,705),(210,729),(1230,729),(1230,705)],7)]
    phases = [
        ('Validate the source', 'Check pinned archive and extracted-text hashes before loading the measurements.',
         f"Source: UCI Household Power Consumption, {quality['source_rows']:,} timestamped rows."),
        ('Process records with RDDs', 'Parse and filter records, emit submeter keys, then combine counts with reduceByKey.',
         f"{quality['rejected_rows']:,} missing-measurement rows stay visible in rejection accounting."),
        ('Create typed DataFrames', 'Keep the schema explicit and derive observed energy, coverage and hourly features.',
         'Missing measurements do not become zero consumption.'),
        ('Query and calibrate', 'Spark SQL summarizes usage. Separate earlier years fit and calibrate the static profiles.',
         'Train through 2008, calibrate on 2009, evaluate held-out 2010 data.'),
        ('Replay real observations', 'Clean minute records arrive as files that Spark reads through Structured Streaming.',
         'File replay simulates streaming. This is historical telemetry, rather than a live utility feed.'),
        ('Aggregate by event time', 'Filter invalid power values, track a watermark and update one-hour aggregates.',
         'Late admission depends on event time and retained window state.'),
        ('Apply coverage before scoring', 'Join the preselected weekly profile and score only windows with enough readings.',
         f"{validation['eligible_windows']:,} eligible windows. {validation['insufficient_coverage_windows']} windows remain unscored."),
        ('Verify outputs after restart', 'Recover the checkpoint and compare window keys, counts and energy with a batch reference.',
         'Batch/stream parity: PASS. Retain SQL tables, progress logs, figures and screenshots.'),
    ]

    def render(stage, progress=0, overview=False):
        image = Image.new('RGB', (W,H), C['bg']);d=ImageDraw.Draw(image)
        d.text((60,30),'GridPulse',font=F['brand'],fill=C['ink'])
        d.text((60,87),'How household telemetry flows through Apache Spark',font=F['subtitle'],fill=C['muted'])
        d.text((1380,58),'PySpark 3.5.3',font=F['body'],fill=C['teal'],anchor='ra')
        d.line((60,127,1380,127),fill='#D3DFE6',width=2)
        if overview:
            heading='The complete batch and streaming workflow'
            sub='Measured outputs inform the labels. Moving records illustrate the dataflow.'
        else:
            heading=f'{stage+1:02d}  {phases[stage][0]}'
            sub='Batch processing' if stage<4 else 'Structured Streaming'
        d.text((60,151),heading,font=F['phase'],fill=C['ink'])
        d.text((60,193),sub,font=F['body'],fill=C['muted'])
        for points, activation in edges:
            arrow(d,points,active=(not overview and stage==activation),done=overview or stage>activation,progress=progress)
        label(d,(365,425),'Clean Parquet readings feed the file replay')
        label(d,(950,465),'Static profile')
        label(d,(500,717),'Independent batch reference from the replayed records')
        for i,(xy,title,lines) in enumerate(nodes):
            node(d,xy,i+1,title,lines,active=(not overview and i==stage),done=overview or i<stage)
        # The timeline gives a stable visual cue without flashes or strobing.
        for i in range(8):
            x=60+i*167
            d.rounded_rectangle((x,755,x+151,761),3,fill=C['teal'] if overview or i<=stage else '#D9E3EA')
        detail='RDD processing feeds typed analytics, while real records and static profiles feed streaming.' if overview else phases[stage][1]
        lines=textwrap.wrap(detail,width=112)
        for i,line in enumerate(lines):d.text((60,782+i*30),line,font=F['detail'],fill=C['ink'])
        context='Chronological calibration, coverage-aware decisions and independent batch reconciliation.' if overview else phases[stage][2]
        assert d.textlength(context,font=F['small'])<=1320
        d.text((60,849),context,font=F['small'],fill=C['muted'])
        d.text((60,878),'Schematic animation. Playback speed does not represent processing time. Executed on local[4].',font=F['small'],fill=C['muted'])
        return image

    DEST.mkdir(parents=True,exist_ok=True)
    overview=render(7,overview=True)
    overview.save(DEST/'workflow.png',optimize=True)
    # A single palette keeps all text/background colors stable between frames.
    samples=Image.new('RGB',(W,H*9))
    samples.paste(overview,(0,0))
    for stage in range(8):samples.paste(render(stage,.5),(0,H*(stage+1)))
    palette=samples.quantize(colors=256,method=Image.Quantize.MEDIANCUT)
    frames=[]
    for stage in range(8):
        frame_count=25 if stage==0 else (40 if stage==7 else 30)
        for frame in range(frame_count):
            frames.append(render(stage,frame/30).quantize(palette=palette,dither=Image.Dither.NONE))
    frames[0].save(DEST/'workflow.gif',save_all=True,append_images=frames[1:],
                   duration=100,loop=0,optimize=True,disposal=1,
                   comment=b'GridPulse schematic workflow. Metrics from saved project outputs. Playback is illustrative.')
    # Verify the encoded file itself, not only the pre-encoding frames.
    with Image.open(DEST/'workflow.gif') as gif:
        assert gif.is_animated and gif.info['loop']==0
        duration=0
        for index in range(gif.n_frames):
            gif.seek(index);gif.load();duration+=gif.info.get('duration',0)
        summary={'width':W,'height':H,'frames':gif.n_frames,'duration_ms':duration,
                 'bytes':(DEST/'workflow.gif').stat().st_size,'loop':'infinite',
                 'metrics_source':['outputs/quality.json','outputs/streaming/validation.json']}
    print(json.dumps(summary,indent=2))


if __name__=='__main__':
    main()
