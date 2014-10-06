#!/usr/bin/env python3
# coding: utf-8
#
# Copyright (c) 2014, Torbjörn Lönnemark <tobbez@ryara.net>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

import sys
import struct
from collections import OrderedDict

class ThriftDecoder:
    type_handlers = {}
    type_names = {}
    message_types = {}

    def __init__(self):
        self.structs = OrderedDict()
        self.fields = []
        self.fp = None
        self.version = None
        self.message_type = None
        self.method_name = None
        self.sequence_id = None

        self.struct_name_counter = 0

        self.message_types[1] = 'call'
        self.message_types[2] = 'reply'
        self.message_types[3] = 'oneway'
        # Handlers
        self.type_handlers[2]  = self.read_bool
        self.type_handlers[3]  = self.read_byte
        self.type_handlers[4]  = self.read_double

        self.type_handlers[6]  = self.read_i16
        self.type_handlers[8]  = self.read_i32
        self.type_handlers[10] = self.read_i64
        
        self.type_handlers[11] = self.read_string
        self.type_handlers[12] = self.read_struct
        self.type_handlers[13] = self.read_map
        self.type_handlers[14] = self.read_set
        self.type_handlers[15] = self.read_list
        self.type_handlers[16] = self.read_enum

        # Names
        self.type_names[2]  = "bool"
        self.type_names[3]  = "byte"
        self.type_names[4]  = "double"

        self.type_names[6]  = "i16"
        self.type_names[8]  = "i32"
        self.type_names[10] = "i64"
        
        self.type_names[11] = "string"
        self.type_names[12] = "struct"
        self.type_names[13] = "map"
        self.type_names[14] = "set"
        self.type_names[15] = "list"
        self.type_names[16] = "enum"

    def __str__(self):
        lines = [
                'version: {}'.format(self.version),
                'type:    {} ({})'.format(self.message_type, self.message_types[self.message_type]),
                'method:  {}'.format(self.method_name),
                'seq id:  {}'.format(self.sequence_id),
                '',
                'Message:'
            ] + [self._format_field(f) for f in self.fields]

        if self.structs:
            lines += [
                '',
                'Structs:'
            ]

            for struct_name, struct_fields in self.structs.items():
                lines += ['{}:'.format(struct_name), '']
                lines += [self._format_field(f) for f in struct_fields]

        return '\n'.join(lines)

    def _format_field(self, f, indent=2):
        return '{}id: {:4}     type: {:10} value: {}'.format(' ' * indent, f[0], self.type_names[f[1]], f[2]) 

    def decode(self, fn):
        self.fp = open(fn, 'rb')

        self.read_header()

    def unpack_one(self, fmt, bytes):
        return struct.unpack(fmt, bytes)[0]

    def read_header(self):
        VERSION_1 = b'\x80\x01'
        self.version = self.read_i16()
        if self.version != self.unpack_one('!h', VERSION_1):
            print("Warning: unexpected version {}", file=sys.stderr)

        self.message_type = self.read_i16()
        self.method_name = self.read_string()
        self.sequence_id = self.read_i32()

        self.fields = self.read_struct()

    def read_type(self):
        return self.read_byte()

    def read_field_id(self):
        return self.read_i16()

    def read_bool(self):
        return bool(self.read_byte())

    def read_byte(self):
        return self.unpack_one('!B', self.fp.read(1))

    def read_double(self):
        raise NotImplementedError

    def read_i16(self):
        return self.unpack_one('!h', self.fp.read(2))

    def read_i32(self):
        return self.unpack_one('!i', self.fp.read(4))

    def read_i64(self):
        return self.unpack_one('!q', self.fp.read(8))

    def read_string(self):
        length = self.read_i32()
        return self.unpack_one('{}s'.format(length), self.fp.read(length)).decode('utf-8')

    def read_struct(self):
        fields = []
        t = self.read_type()
        while t != 0:
            field_id = self.read_field_id()
            field_data = self.type_handlers[t]()

            if t == 12:
                struct_name = 'UnknownStruct{}'.format(self.struct_name_counter)
                self.struct_name_counter += 1

                self.structs[struct_name] = field_data
                fields.append((field_id, t, struct_name))
            else:
                fields.append((field_id, t, field_data))
            
            t = self.read_type()
        return tuple(fields)

    def read_map(self):
        raise NotImplementedError

    def read_set(self):
        raise NotImplementedError

    def read_list(self):
        raise NotImplementedError

    def read_enum(self):
        raise NotImplementedError

class DecoderApp:
    def run_app(self, args):
        if len(args) <= 1 or '-h' in args[1:] or '--help' in args[1:]:
            print('usage: {} <file> [file, ...]'.format(args[0]))
            return
        input_files = args[1:]
        decoded = []
        for fn in input_files:
            d = ThriftDecoder()
            d.decode(fn)
            decoded.append(str(d))
        print('\n\n'.join(decoded))


if __name__ == '__main__':
    DecoderApp().run_app(sys.argv)
