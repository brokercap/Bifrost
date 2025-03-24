package mongo

import (
	"github.com/rwynn/gtm/v2"
)

func (c *MongoInput) GetTransactionApplyOpsList(op *gtm.Op) (ops []*gtm.Op) {
	if !op.IsCommand() || op.Data == nil {
		return
	}
	if applyOps, ok := op.Data["applyOps"].([]interface{}); ok {
		for _, applyOp := range applyOps {
			if opData, ok := applyOp.(map[string]interface{}); ok {
				namespace, _ := opData["ns"].(string)
				operation, _ := opData["op"].(string)
				var data map[string]interface{}
				var id interface{}
				var b bool
				if data, b = opData["o"].(map[string]interface{}); !b {
					continue
				}
				switch operation {
				case "i", "d":
					id = data["_id"]
				case "u":
					if update, ok := opData["o2"].(map[string]interface{}); ok {
						id = update["_id"]
					} else {
						continue
					}
				default:
					continue
				}
				newOp := &gtm.Op{
					Id:                id,
					Operation:         operation,
					Namespace:         namespace,
					Data:              data,
					Timestamp:         op.Timestamp,
					Source:            op.Source,
					Doc:               data,
					UpdateDescription: nil,
					ResumeToken:       op.ResumeToken,
				}
				ops = append(ops, newOp)
			}
		}
	}
	return
}
