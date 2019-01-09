/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package frames

import (
	"testing"
)

func TestBitArray(t *testing.T) {
	size := 13
	ba, err := NewBitArray(size)
	if err != nil {
		t.Fatal(err)
	}

	if len(ba.data) != 2 {
		t.Fatalf("bad # of bytes - %d != 2", len(ba.data))
	}

	if ba.Len() != size {
		t.Fatalf("bad length %d != %d", ba.Len(), size)
	}

	if err := ba.Set(3, true); err != nil {
		t.Fatal(err)
	}

	if err := ba.Set(3, false); err != nil {
		t.Fatal(err)
	}

	setBits := map[int]bool{7: true, 5: true}
	for b := range setBits {
		if err := ba.Set(b, true); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < ba.Len(); i++ {
		val, err := ba.Get(i)
		if err != nil {
			t.Fatal(err)
		}

		if val != setBits[i] {
			t.Fatalf("bad value at index %d: %v", i, val)
		}
	}

	s := ba.String()
	if s != "0000010100000" {
		t.Fatalf("bad bits: %s", s)
	}

	if err := ba.Set(ba.Len(), true); err == nil {
		t.Fatal("set out of bounds succeeded")
	}
}
