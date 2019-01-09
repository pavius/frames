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
	"bytes"
	"fmt"
)

var (
	zero = []byte{'0'}
	one  = []byte{'1'}
)

// BitArray is a bit array
type BitArray struct {
	data []byte
	size int
}

// NewBitArray returns a new BitArray with given size
func NewBitArray(size int) (*BitArray, error) {
	if size < 1 {
		return nil, fmt.Errorf("size must be >= 1 (got %d)", size)
	}

	nbytes := (size + 7) / 8
	ba := &BitArray{
		size: size,
		data: make([]byte, nbytes),
	}

	return ba, nil
}

// Get gets the value at index i
func (ba *BitArray) Get(i int) (bool, error) {
	if err := ba.checkInbounds(i); err != nil {
		return false, err
	}

	byteNum, bitNum := ba.loc(i)
	val := ba.data[byteNum] >> bitNum
	return (val & 1) == 1, nil
}

// Set sets value at location i
func (ba *BitArray) Set(i int, value bool) error {
	if err := ba.checkInbounds(i); err != nil {
		return err
	}

	byteNum, bitNum := ba.loc(i)
	if value {
		ba.data[byteNum] |= (1 << bitNum)
	} else {
		ba.data[byteNum] &= ^(1 << bitNum)
	}

	return nil
}

// Len is the size in bits
func (ba *BitArray) Len() int {
	return ba.size
}

func (ba *BitArray) String() string {
	var buf bytes.Buffer
	for i := ba.Len() - 1; i >= 0; i-- {
		val, _ := ba.Get(i)
		if val {
			buf.Write(one)
		} else {
			buf.Write(zero)
		}
	}

	return buf.String()
}

func (ba *BitArray) loc(i int) (int, uint) {
	return i / 8, uint(i % 8)
}

func (ba *BitArray) checkInbounds(i int) error {
	if i >= 0 && i < ba.size {
		return nil
	}

	return fmt.Errorf("index should be [0:%d], got %d", ba.size-1, i)
}
