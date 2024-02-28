// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package request

import "github.com/SmartBFT-Go/consensus/pkg/api"

type OnlyIDRequestInspector struct {
	RequestInspector api.RequestInspector
}

func (ri *OnlyIDRequestInspector) RequestID(req []byte) string {
	return ri.RequestInspector.RequestID(req).ID
}
