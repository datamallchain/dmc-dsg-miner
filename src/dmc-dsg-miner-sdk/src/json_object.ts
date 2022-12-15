import {
    BodyContent,
    BodyContentDecoder,
    BuckyBuffer,
    BuckyBufferDecoder,
    BuckyNumber,
    BuckyNumberDecoder,
    BuckyResult,
    DescContent,
    DescContentDecoder,
    DescTypeInfo,
    HashValue,
    HashValueDecoder,
    NamedObject,
    NamedObjectBuilder,
    NamedObjectDecoder,
    NamedObjectDesc,
    NamedObjectDescDecoder,
    ObjectId,
    SubDescType
} from "cyfs-sdk";
import {Ok} from "ts-results";

const JSONObjectType = 50001;

export class JSONObjectTypeInfo extends DescTypeInfo {
    obj_type(): number {
        return JSONObjectType;
    }

    sub_desc_type(): SubDescType {
        return {
            owner_type: "option",
            area_type: "disable",
            author_type: "disable",
            key_type: "disable"
        };
    }

}

export class JSONObjectDescContent extends DescContent {
    constructor(
        public obj_type: number,
        public content_hash: HashValue
    ) {
        super();
    }

    raw_encode(buf: Uint8Array): BuckyResult<Uint8Array> {
        buf = new BuckyNumber("u16", this.obj_type).raw_encode(buf).unwrap();
        buf = this.content_hash.raw_encode(buf).unwrap();
        return Ok(buf);
    }

    raw_measure(): BuckyResult<number> {
        let size = 0;
        size += new BuckyNumber("u16", this.obj_type).raw_measure().unwrap();
        size += this.content_hash.raw_measure().unwrap();
        return Ok(size);
    }

    type_info(): DescTypeInfo {
        return new JSONObjectTypeInfo();
    }

}

export class JSONObjectDescContentDecoder extends DescContentDecoder<JSONObjectDescContent> {
    raw_decode(buf: Uint8Array): BuckyResult<[JSONObjectDescContent, Uint8Array]> {
        let obj_type;
        {
            const ret = new BuckyNumberDecoder("u16").raw_decode(buf);
            if (ret.err) {
                return ret;
            }

            [obj_type, buf] = ret.unwrap();
        }

        let content_hash: HashValue;
        {
            const ret = new HashValueDecoder().raw_decode(buf);
            if (ret.err) {
                return ret;
            }

            [content_hash, buf] = ret.unwrap();
        }

        const result: [JSONObjectDescContent, Uint8Array] = [new JSONObjectDescContent(obj_type.toNumber(), content_hash), buf];
        return Ok(result);
    }

    type_info(): DescTypeInfo {
        return new JSONObjectTypeInfo();
    }
}

export class JSONObjectBodyContent extends BodyContent {
    constructor(public data: Uint8Array) {
        super();
    }

    raw_encode(buf: Uint8Array): BuckyResult<Uint8Array> {
        return Ok(new BuckyBuffer(this.data).raw_encode(buf).unwrap());
    }

    raw_measure(): BuckyResult<number> {
        return Ok(new BuckyBuffer(this.data).raw_measure().unwrap());
    }
}

export class JSONObjectBodyContentDecoder extends BodyContentDecoder<JSONObjectBodyContent> {
    raw_decode(buf: Uint8Array): BuckyResult<[JSONObjectBodyContent, Uint8Array]> {
        let body;
        {
            const ret = new BuckyBufferDecoder().raw_decode(buf);
            if (ret.err) {
                return ret;
            }
            [body, buf] = ret.unwrap();
        }

        const result: [JSONObjectBodyContent, Uint8Array] = [new JSONObjectBodyContent(body.buffer), buf];
        return Ok(result);
    }

}

export class JSONObjectDesc extends NamedObjectDesc<JSONObjectDescContent> {
}

export class DSGJSONObjectDescDecoder extends NamedObjectDescDecoder<JSONObjectDescContent> {
    constructor() {
        super(new JSONObjectDescContentDecoder());
    }
}

export class JSONObjectBuilder extends NamedObjectBuilder<JSONObjectDescContent, JSONObjectBodyContent> {
}

export class JSONObject extends NamedObject<JSONObjectDescContent, JSONObjectBodyContent> {
    static create(dec_id: ObjectId, owner_id: ObjectId, obj_type: number, data: Uint8Array): JSONObject {
        const body = new JSONObjectBodyContent(data);
        const desc = new JSONObjectDescContent(obj_type, HashValue.hash_data(data));
        const builder = new JSONObjectBuilder(desc, body);

        return builder.dec_id(dec_id).build(JSONObject);
    }
}

export class JSONObjectDecoder extends NamedObjectDecoder<JSONObjectDescContent, JSONObjectBodyContent, JSONObject> {
    constructor() {
        super(new JSONObjectDescContentDecoder(), new JSONObjectBodyContentDecoder(), JSONObject);
    }

    raw_decode(buf: Uint8Array): BuckyResult<[NamedObject<JSONObjectDescContent, JSONObjectBodyContent>, Uint8Array]> {
        return super.raw_decode(buf).map((r: any) => {
            const [obj, _buf] = r;
            const sub_obj = new JSONObject(obj.desc(), obj.body(), obj.signs(), obj.nonce());
            return [sub_obj, _buf];
        });
    }

}
