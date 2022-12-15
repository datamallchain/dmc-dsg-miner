import {BuckyResult, error, NONAPILevel, NONObjectInfo, ObjectId, SharedCyfsStack} from "cyfs-sdk";
import {JSONObject, JSONObjectDecoder} from "./json_object";
import {Ok} from "ts-results";

export interface MinerStat {
    bill_count: string;
    order_count: string;
    billed_space: string;
    selled_space: string;
    used_space: string;
};

export class DmcDsgMinerClient {
    stack: SharedCyfsStack;
    dec_id: ObjectId;

    constructor(stack: SharedCyfsStack, dec_id: ObjectId) {
        this.stack = stack;
        this.dec_id = dec_id;
    }

    private async request(obj_type: number, req_data?: any, target?: ObjectId): Promise<BuckyResult<any>> {
        const ret = await this.stack.util().get_device({common: {flags: 0}});
        if (ret.err) {
            error("request err", ret, " obj_type ", obj_type);
            return ret;
        }
        const {device_id} = ret.unwrap();

        let send_content;
        if (req_data) {
            const encoder = new TextEncoder();
            send_content = encoder.encode(JSON.stringify(req_data));
        } else {
            send_content = new Uint8Array();
        }
        const obj = JSONObject.create(this.dec_id, device_id.object_id, obj_type, send_content);
        const obj_id = obj.desc().calculate_id();
        const obj_data = new Uint8Array(obj.raw_measure().unwrap());
        obj.raw_encode(obj_data).unwrap();
        const result = await this.stack.non_service().post_object({
            "common": {
                "req_path": "dsg_local_commands",
                "dec_id": this.dec_id,
                "level": NONAPILevel.Router,
                "flags": 0,
                "target": target,
            }, "object": new NONObjectInfo(obj_id, obj_data)
        });
        if (result.err) {
            error("request err", result, " obj_type ", obj_type);
            return result;
        }

        const ret_obj = new JSONObjectDecoder().raw_decode(result.unwrap().object!!.object_raw);
        if (ret_obj.err) {
            error("request err", ret_obj, " obj_type ", obj_type);
            return ret_obj;
        }

        const [ret_json_obj] = ret_obj.unwrap();
        const decoder = new TextDecoder();
        const data = decoder.decode(ret_json_obj.body().unwrap().content().data);
        const json_obj = JSON.parse(data);

        return Ok(json_obj);
    }

    async get_stat(): Promise<BuckyResult<MinerStat>> {
        return await this.request(12);
    }
}
