#!/usr/bin/env python
import sys
import traceback
import numpy as np
import logging
from keras.applications.resnet50 import ResNet50
from keras.preprocessing import image
from keras.applications.resnet50 import preprocess_input, decode_predictions
from io import BytesIO
from flask import Flask, jsonify, request
from gevent import pywsgi

# adjust server logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.DEBUG)

# load the pre-trained model for imagenet
model = ResNet50(weights='imagenet')

# flask and server
app = Flask(__name__)
server = None



# returns Veritone series object from the classname and the confidence
def to_series(classname, confidence, startMS, stopMS):
    boundingPoly = [
        {'x': 0, 'y': 0},
        {'x': 1, 'y': 0},
        {'x': 1, 'y': 1},
        {'x': 0, 'y': 1}
    ]
    obj = {
        'label': classname,
        'type': 'object',
        'boundingPoly': boundingPoly
    }
    seriesItems = [{
        'confidence': confidence,
        'startTimeMs': int(startMS),
        'stopTimeMs': int(stopMS),
        'found': classname,
        'object':  obj
    }]
    return {'series': seriesItems}


@app.route('/')
def index():
    return readyz()

@app.route('/readyz', methods=['GET'])
def readyz():
    return 'OK', 200


@app.route('/process', methods=['POST'])
def process():
    try:
        # load the chunk and save it to a buffer
        file = request.files['chunk']
        chunk = BytesIO()
        file.save(chunk)

        # offset params for the frame
        startOffsetMS = request.form['startOffsetMS']
        endOffsetMS = request.form['endOffsetMS']
        
        # you can use width and height to calculate bounding poly
        # width = request.form['width']
        # height = request.form['height']

        # this engine only support images so ignore different mime types
        chunkMimeType = request.form['chunkMimeType']
        if not chunkMimeType.startswith('image/'):
            return "ignore", 204

        # pre process the image features
        img = image.load_img(chunk, target_size=(224, 224))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)

        # make the prediction
        preds = model.predict(x)
        # decode predictions and get the most probable class and confidence
        decoded = decode_predictions(preds, top=1)[0]
        image_class = decoded[0][1]
        image_confidence = float(decoded[0][2])
 
        series = to_series(image_class, image_confidence, startOffsetMS, endOffsetMS)
        return jsonify(series)
    except:
        tb = traceback.format_exc()        
        app.logger.error('error predicting %s', str(tb))
        return str(tb), 500


@app.route('/shutdown', methods=['DELETE'])
def shutdown():
    app.logger.info('shuting the server down')
    server.stop()
    app.logger.info('server terminated')
    return 'OK', 200    

if __name__ == '__main__':
    port = 8080
    server = pywsgi.WSGIServer(('0.0.0.0', int(port)), app)
    server.serve_forever()
