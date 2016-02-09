import re
import os
import sys
import random
import subprocess
import predictionio

from datetime import datetime
from optparse import OptionParser


GENDER_WOMEN = re.compile(r'\/women\/', re.I)
GENDER_MEN = re.compile(r'\/men\/', re.I)
CAT = re.compile(r'\/[a-zA-Z0-9\s\&\-]+\/cat\/', re.I)
COLOR = re.compile(r'\/[a-zA-Z\s\-]+\/image[a-zA-Z0-9]+.jpg\"?$', re.I)
IID = re.compile(r'iid\=[0-9]+', re.I)
TZ = predictionio.pytz.timezone("Europe/Warsaw")


class FeatureExtractor():
    def __init__(self):
        self.raw = {}
        self.keys = []
        self.source = None

    def extract_raw(self, source, keys):
        self.source = source
        self.keys = [k.replace('/', '_').strip() for k in keys]
        return dict(zip(self.keys, self.source))

    def extract_features(self, source, keys):
        self.raw = self.extract_raw(source, keys)
        try:
            return True, {
                'iid': self._extract_iid(),
                'gender': self._extract_gender(),
                'category': self._extract_category(),
                'color': self._extract_color(),
                'brand': self._extract_brand(),
                'description': re.sub(r'^"|"$', '', self.raw['description']),
                'image': re.sub(r'^"|"$', '', self.raw['image']),
                'price': float(re.sub(r'^"?\$|"?$', '', self.raw['price']))
                }
        except Exception as error:
            return False, {'error': error}

    def _extract_gender(self):
        if GENDER_WOMEN.search(self.raw['_url']):
            return 'women'
        if GENDER_MEN.search(self.raw['_url']):
            return 'men'        
        return 'unspecified'

    def _extract_category(self):
        return CAT.findall(self.raw['_url'])[0].split('/')[1]

    def _extract_color(self):
        return COLOR.findall(self.raw['image'])[0].split('/')[1]

    def _extract_brand(self):
        from_link = self.raw['link'].replace('http://www.asos.com/', '') \
                                    .split('/', 2)[1]
        from_link_patt = r'^' + from_link.replace('-', r'\"? (?: \-|\s)?')
        results = re.findall(from_link_patt, self.raw['description'], re.I)
        if len(results) > 0:
            return results[0]
        return from_link

    def _extract_iid(self):
        return int(IID.findall(self.raw['link'])[0].replace('iid=', ''))


class EventHandler(object):
    def __init__(self, event_server_uri, access_key):
        self.client = predictionio.EventClient(access_key, event_server_uri)
        self.exporter = None
        self.filename = None

    def delete_events(self):
        try:
            for event in self.client.get_events():
                self.client.adelete_event(event['eventId'])
        except predictionio.NotFoundError:
            return

    def _do_create_event(self, func, event, entity_type, entity_id,
                         target_entity_type, target_entity_id, properties):
        return func(event=event,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    target_entity_type=target_entity_type,
                    target_entity_id=target_entity_id,
                    properties=properties,
                    event_time=datetime.now(TZ))

    def create_event(self, event, entity_type, entity_id,
                     target_entity_type=None, target_entity_id=None,
                     properties=None, **kwargs):
        async = kwargs.get('async',  False)
        if async:
            func = self.client.acreate_event
        else:
            func = self.client.create_event
        return self._do_create_event(func, event, entity_type, entity_id,
                                     target_entity_type, target_entity_id,
                                     properties)

    def _lazy_get_exporter_func(self, **kwargs):
        """kwargs must contain filename"""
        if self.exporter is None:
            self.filename = kwargs['filename']
            self.exporter = predictionio.FileExporter(file_name=self.filename)
        return self.exporter.create_event
        
    def export_event(self, event, entity_type, entity_id,
                     target_entity_type=None, target_entity_id=None,
                     properties=None, **kwargs):
        func = self._lazy_get_exporter_func(**kwargs)
        return self._do_create_event(func, event, entity_type, entity_id,
                                     target_entity_type, target_entity_id,
                                     properties)

    def close(self):
        if self.exporter is not None:
            subprocess.Popen(['pio', 'import',
                              '--appid', '1', #XXX: how to get app id?
                              '--input', self.filename])
            self.exporter.close()


def main(filename, **kwargs):
    delimiter = kwargs.get('delimiter', ',')
    export_json = kwargs.get('export_json', False)
    clean = kwargs.get('clean', False)
    event_server_uri = kwargs['event_server_uri']
    access_key = kwargs['access_key']

    handler = EventHandler(event_server_uri, access_key)
    if clean:
        handler.delete_events()
    if export_json:
        handler_method = handler.export_event
        kwargs = {'filename': filename.rsplit('.', 1)[0] + '.json'}
    else:
        handler_method = handler.create_event
        kwargs = {}

    users = ['usr%d' % i for i in range(10)]
    events = []
    props = []

    extractor = FeatureExtractor()
    f = open(filename, "r+")
    keys = f.readline().split(delimiter)
    ln = 1
    for line in f:
        ln += 1

        # extract features
        source = line.split(delimiter)
        success, result = extractor.extract_features(source, keys=keys)
        if not success:
            print  >> sys.stderr, "[ERROR] line %d: %s" % (ln, result['error'])
            continue

        # create $set event on predioction.io server
        item_id = result.pop('iid')
        item_descr = result.pop('description')
        entity_id = "%s (%s)" % (item_descr, item_id)
        for key, val in result.items():
            handler_method(event='$set',
                           entity_type='item',
                           entity_id=entity_id,
                           properties={key: val},
                           **kwargs)
            props.append([str(entity_id), '$set', "%s:%s" % (key, val)])

        # flip a coin to create user's event (purchase | view)
        if random.random() > .5:
            user = users[random.randint(0, len(users)-1)] # random user
            event = 'view' if random.random() > .5 else 'purchase' # coin for event
            handler_method(event=event,
                           entity_type='user',
                           entity_id=user,
                           properties={},
                           target_entity_id=entity_id,
                           target_entity_type='item',
                           **kwargs)
            events.append([user, event, entity_id])
    f.close()
    handler.close()
    if handler.filename is not None:
        print >> sys.stdout, '--\nExported to %s, waiting in queue' % \
                             os.path.abspath(handler.filename)

    # export props and events to text file
    f = open(filename.rsplit('.', 1)[0] + '.txt', 'w+')
    for line in (events + props):
        f.write(','.join(line) + '\n')
    f.close()

    # # train & deploy
    # commands = ['pio train', 'pio deploy']
    # for command in commands:
    #     args = command.split(' ')
    #     status = subprocess.check_call(args)
    #     if status != 0:
    #         print  >> sys.stderr, "[ERROR] Command '%s' returned non-zero exit status %s" % (command, status)
    #         return


if __name__ == '__main__':
    parser = OptionParser(usage="usage: python %prog [OPTIONS] resource_filename")
    parser.add_option("-d", "--delimiter", action="store", dest="delimiter",
                      default=',', help="Delimiter [default \'%default\']")
    parser.add_option("-s", "--server", action="store", dest="event_server_uri",
                      help="URI of event server")
    parser.add_option("-c", "--access", action="store", dest="access_key",
                      help="Access key")
    parser.add_option("-l", "--clean", action="store_true", dest="clean",
                      help="Clean before export")
    parser.add_option("-e", "--export", action="store_true", dest="export_json",
                      help="Export to json")
    opts, args = parser.parse_args()
    if not opts.event_server_uri:
        parser.error('URI of event server missing')
    if not opts.access_key:
        parser.error('Access key missinng')

    try:
        main(args[0], **vars(opts))
        print 'Done.'
    except IndexError:
        print >> sys.stderr, "Resource filename missing"
    except IOError as error:
        print >> sys.stderr, error
