# -*- coding: utf-8 -*-
import sys
import socket
import select
import logging
from time import time,sleep
from struct import pack, unpack

sys.path.append('/usr/lib/yandex/m3-monitor/lib/python3.4/site-packages')

from fsmsock.proto import UdpTransport

class BacnetUdpClient(UdpTransport):
    VLC_BACNET_IP_ANNEXJ = 0x81
    VLC_ORIGINAL_UNICAST_NPDU = 0x0a

    NPDU_ASHRAE_135 = 0x01

    APDU_CONFIRMED_REQ = 0x00
    APDU_COMPLEX_ACQ   = 0x03
    APDU_SIZE_1476 = 0x05

    APDU_READ_PROPERTY = 12

    APDU_OBJ_TYPE_ANALOG = 2
    APDU_OBJ_TYPE_BINARY = 5
    APDU_OBJ_TYPE_DEVICE = 8

    APDU_VAL_TYPE_REAL = 4
    APDU_VAL_TYPE_ENUMERATED = 9

    APDU_PROP_ID_PRESENT_VALUE = 85

    APDU_TAG_CONTEXT = 8

    def __init__(self, host, interval, device, props, port=0xBAC0):
        self._props = props
        self._device = device
        super().__init__(host, interval, port)

    def _build_buf(self):
        self._res = {}
        self._buf = []
        self._bufidx = 0
        self._seqid = 1
        npdu = pack('2B', self.NPDU_ASHRAE_135, 0x04)
        for k,p in self._props.items():
            apdu = pack('>5BI2B',
                self.APDU_CONFIRMED_REQ << 4,
                self.APDU_SIZE_1476,
                self._seqid,
                self.APDU_READ_PROPERTY,
                0x0 << 4 | self.APDU_TAG_CONTEXT | 0x4, # Tag number: 0, Length value type: 4
                p[1] << 22 | p[0],
                0x1 << 4 | self.APDU_TAG_CONTEXT | 0x1, # Tag number: 1, Length value type: 1
                self.APDU_PROP_ID_PRESENT_VALUE)
            req = pack('>2BH', self.VLC_BACNET_IP_ANNEXJ, self.VLC_ORIGINAL_UNICAST_NPDU, len(npdu) + len(apdu) + 4) + npdu + apdu
            self._buf.append(req)
            p.append(k)
            self._res[self._seqid] = p
            self._seqid += 1

    def send_buf(self):
        if not len(self._buf):
            return 0
        return self._write(self._buf[self._bufidx])

    def on_unorder(self, data):
        return self.process_data(data)

    def _next(self, rc=True):
        self._bufidx = (self._bufidx + 1) % len(self._buf)
        if rc:
            self._state = self.READY
        if self._bufidx == 0:
            return self.stop()
        return rc

    def process_data(self, data, tm = None):
        self._retries = 0
        if not data:
            return False
        if tm is None:
            tm = time()

        if len(data) < 4: # Too small packet
            return self._next()
        # Check VLC
        if data[0] != self.VLC_BACNET_IP_ANNEXJ and \
           data[1] != self.VLC_ORIGINAL_UNICAST_NPDU:
            return self._next()
        if len(data) != unpack('>H', data[2:4])[0]: # BACNet packet length incorrect
            return self._next()

        # Check NPDU
        if data[4] != self.NPDU_ASHRAE_135:
            return self._next()

        apdu = data[6:]
        if len(apdu) < 13:
            return self._next()

        if (apdu[0] >> 4) != self.APDU_COMPLEX_ACQ:
            return self._next()

        prop = self._res.get(apdu[1], None)
        if prop is None: # Unknown Invoke ID in answer
            return self._next()

        if apdu[2] != self.APDU_READ_PROPERTY:
            return self._next() # Unknown Service choice

        if apdu[3] != 0x0c: # Context tag 0, Context specific tag, LVT: 4
             return self._next()

        obj = unpack('>I', apdu[4:8])[0]
        if (obj >> 22) != prop[1] or (obj & 0x3fffff) != prop[0]: # Object type != prop type or Instance ID != prop ID
             return self._next()

        if apdu[8] != 0x19: # Context tag: 1, Context specific tag, LVT: 1
             return self._next()

        if apdu[9] != self.APDU_PROP_ID_PRESENT_VALUE:
             return self._next()

        if apdu[10] != 0x3e: # Context tag: 3, Context specific tag, Named tag: opened tag
             return self._next()

        lvt = apdu[11] & 0x07
        val_type = apdu[11] >> 4
        val = apdu[12:12+lvt]
        if val_type == self.APDU_VAL_TYPE_REAL and lvt == 4:
            val = unpack('>f', val)[0]
        elif val_type == self.APDU_VAL_TYPE_ENUMERATED and lvt == 1:
            val = val[0]
        else:
            return self._next()

        self.on_data(prop[2], val, tm)

        return self._next()

    def on_data(self, points, response, tm):
        pass

def main():
    cfg = {
        'host': '37.9.91.58',
        'interval': 3.0,
        'device': 77000,
        'props': {
            'A001' : [    1, BacnetUdpClient.APDU_OBJ_TYPE_ANALOG ],
            'I132' : [ 1132, BacnetUdpClient.APDU_OBJ_TYPE_ANALOG ],
            'D169' : [  169, BacnetUdpClient.APDU_OBJ_TYPE_BINARY ],
        }
    }
    from fsmsock import async
    c = BacnetUdpClient(**cfg)
    fsm = async.FSMSock()
    fsm.connect(c)
    while fsm.run():
        fsm.tick()

if __name__ == '__main__':
    main()
