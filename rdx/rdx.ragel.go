//line rdx.rl:1
package rdx

import "fmt"

//line rdx.rl:187

//line rdx.ragel.go:10
var __RDX_actions []byte = []byte{
	0, 1, 0, 1, 1, 1, 2, 1, 3,
	1, 4, 1, 5, 1, 6, 1, 7,
	1, 8, 1, 9, 1, 10, 1, 11,
	1, 12, 1, 13, 1, 14, 1, 15,
	2, 1, 3, 2, 1, 10, 2, 1,
	12, 2, 1, 14, 2, 1, 15, 2,
	2, 10, 2, 2, 12, 2, 2, 14,
	2, 2, 15, 2, 3, 10, 2, 3,
	12, 2, 3, 14, 2, 3, 15, 2,
	4, 10, 2, 4, 12, 2, 4, 14,
	2, 4, 15, 2, 5, 10, 2, 5,
	12, 2, 5, 14, 2, 5, 15, 2,
	6, 10, 2, 6, 12, 2, 6, 14,
	2, 6, 15, 2, 7, 10, 2, 7,
	12, 2, 7, 14, 2, 7, 15, 2,
	8, 10, 2, 8, 12, 2, 8, 14,
	2, 8, 15, 2, 9, 10, 2, 9,
	12, 2, 9, 14, 2, 9, 15, 2,
	11, 0, 2, 11, 10, 2, 11, 12,
	2, 11, 14, 2, 11, 15, 2, 13,
	0, 2, 13, 10, 2, 13, 12, 2,
	13, 14, 2, 13, 15, 3, 1, 3,
	10, 3, 1, 3, 12, 3, 1, 3,
	14, 3, 1, 3, 15,
}

var __RDX_key_offsets []int16 = []int16{
	0, 0, 4, 10, 17, 19, 22, 26,
	28, 31, 34, 36, 39, 45, 52, 54,
	57, 59, 63, 67, 69, 75, 82, 84,
	87, 93, 99, 106, 108, 111, 117, 124,
	132, 138, 144, 151, 153, 156, 162, 169,
	171, 174, 177, 186, 192, 198, 204, 210,
	234, 244, 253, 277, 286, 310, 321, 332,
	347, 356, 368, 377, 397, 414, 423, 439,
	456, 465, 482, 504, 513, 530,
}

var __RDX_trans_keys []byte = []byte{
	10, 13, 34, 92, 48, 57, 65, 70,
	97, 102, 44, 48, 57, 65, 70, 97,
	102, 48, 57, 41, 48, 57, 43, 45,
	48, 57, 48, 57, 41, 48, 57, 43,
	48, 57, 48, 57, 45, 48, 57, 48,
	57, 65, 70, 97, 102, 44, 48, 57,
	65, 70, 97, 102, 48, 57, 41, 48,
	57, 48, 57, 69, 101, 48, 57, 43,
	45, 48, 57, 48, 57, 48, 57, 65,
	70, 97, 102, 44, 48, 57, 65, 70,
	97, 102, 48, 57, 41, 48, 57, 48,
	57, 65, 70, 97, 102, 48, 57, 65,
	70, 97, 102, 44, 48, 57, 65, 70,
	97, 102, 48, 57, 41, 48, 57, 48,
	57, 65, 70, 97, 102, 45, 48, 57,
	65, 70, 97, 102, 43, 45, 48, 57,
	65, 70, 97, 102, 48, 57, 65, 70,
	97, 102, 48, 57, 65, 70, 97, 102,
	44, 48, 57, 65, 70, 97, 102, 48,
	57, 41, 48, 57, 48, 57, 65, 70,
	97, 102, 44, 48, 57, 65, 70, 97,
	102, 48, 57, 41, 48, 57, 41, 48,
	57, 34, 47, 92, 98, 102, 110, 114,
	116, 117, 48, 57, 65, 70, 97, 102,
	48, 57, 65, 70, 97, 102, 48, 57,
	65, 70, 97, 102, 48, 57, 65, 70,
	97, 102, 32, 34, 40, 43, 44, 45,
	58, 91, 93, 95, 123, 125, 9, 13,
	48, 57, 65, 70, 71, 90, 97, 102,
	103, 122, 32, 40, 44, 58, 91, 93,
	123, 125, 9, 13, 32, 44, 58, 91,
	93, 123, 125, 9, 13, 32, 34, 40,
	43, 44, 45, 58, 91, 93, 95, 123,
	125, 9, 13, 48, 57, 65, 70, 71,
	90, 97, 102, 103, 122, 32, 44, 58,
	91, 93, 123, 125, 9, 13, 32, 34,
	40, 43, 44, 45, 58, 91, 93, 95,
	123, 125, 9, 13, 48, 57, 65, 70,
	71, 90, 97, 102, 103, 122, 32, 44,
	58, 91, 93, 123, 125, 9, 13, 48,
	57, 32, 44, 58, 91, 93, 123, 125,
	9, 13, 48, 57, 32, 40, 44, 46,
	58, 69, 91, 93, 101, 123, 125, 9,
	13, 48, 57, 32, 44, 58, 91, 93,
	123, 125, 9, 13, 32, 40, 44, 58,
	91, 93, 123, 125, 9, 13, 48, 57,
	32, 44, 58, 91, 93, 123, 125, 9,
	13, 32, 40, 44, 45, 46, 58, 69,
	91, 93, 101, 123, 125, 9, 13, 48,
	57, 65, 70, 97, 102, 32, 40, 44,
	45, 58, 91, 93, 123, 125, 9, 13,
	48, 57, 65, 70, 97, 102, 32, 44,
	58, 91, 93, 123, 125, 9, 13, 32,
	40, 44, 58, 91, 93, 123, 125, 9,
	13, 48, 57, 65, 70, 97, 102, 32,
	40, 44, 45, 58, 91, 93, 123, 125,
	9, 13, 48, 57, 65, 70, 97, 102,
	32, 44, 58, 91, 93, 123, 125, 9,
	13, 32, 40, 44, 45, 58, 91, 93,
	123, 125, 9, 13, 48, 57, 65, 70,
	97, 102, 32, 40, 44, 45, 58, 91,
	93, 95, 123, 125, 9, 13, 48, 57,
	65, 70, 71, 90, 97, 102, 103, 122,
	32, 44, 58, 91, 93, 123, 125, 9,
	13, 32, 40, 44, 58, 91, 93, 95,
	123, 125, 9, 13, 48, 57, 65, 90,
	97, 122, 32, 44, 58, 91, 93, 123,
	125, 9, 13,
}

var __RDX_single_lengths []byte = []byte{
	0, 4, 0, 1, 0, 1, 2, 0,
	1, 1, 0, 1, 0, 1, 0, 1,
	0, 2, 2, 0, 0, 1, 0, 1,
	0, 0, 1, 0, 1, 0, 1, 2,
	0, 0, 1, 0, 1, 0, 1, 0,
	1, 1, 9, 0, 0, 0, 0, 12,
	8, 7, 12, 7, 12, 7, 7, 11,
	7, 8, 7, 12, 9, 7, 8, 9,
	7, 9, 10, 7, 9, 7,
}

var __RDX_range_lengths []byte = []byte{
	0, 0, 3, 3, 1, 1, 1, 1,
	1, 1, 1, 1, 3, 3, 1, 1,
	1, 1, 1, 1, 3, 3, 1, 1,
	3, 3, 3, 1, 1, 3, 3, 3,
	3, 3, 3, 1, 1, 3, 3, 1,
	1, 1, 0, 3, 3, 3, 3, 6,
	1, 1, 6, 1, 6, 2, 2, 2,
	1, 2, 1, 4, 4, 1, 4, 4,
	1, 4, 6, 1, 4, 1,
}

var __RDX_index_offsets []int16 = []int16{
	0, 0, 5, 9, 14, 16, 19, 23,
	25, 28, 31, 33, 36, 40, 45, 47,
	50, 52, 56, 60, 62, 66, 71, 73,
	76, 80, 84, 89, 91, 94, 98, 103,
	109, 113, 117, 122, 124, 127, 131, 136,
	138, 141, 144, 154, 158, 162, 166, 170,
	189, 199, 208, 227, 236, 255, 265, 275,
	289, 298, 309, 318, 335, 349, 358, 371,
	385, 394, 408, 425, 434, 448,
}

var __RDX_indicies []byte = []byte{
	1, 1, 2, 3, 0, 4, 4, 4,
	1, 5, 4, 4, 4, 1, 6, 1,
	7, 6, 1, 8, 8, 9, 1, 10,
	1, 11, 10, 1, 12, 13, 1, 14,
	1, 12, 15, 1, 16, 16, 16, 1,
	17, 16, 16, 16, 1, 18, 1, 19,
	18, 1, 20, 1, 21, 21, 20, 1,
	22, 22, 23, 1, 23, 1, 24, 24,
	24, 1, 25, 24, 24, 24, 1, 26,
	1, 27, 26, 1, 28, 28, 28, 1,
	29, 29, 29, 1, 30, 29, 29, 29,
	1, 31, 1, 32, 31, 1, 33, 33,
	33, 1, 34, 35, 35, 35, 1, 22,
	36, 37, 35, 35, 1, 38, 28, 28,
	1, 39, 39, 39, 1, 40, 39, 39,
	39, 1, 41, 1, 42, 41, 1, 43,
	43, 43, 1, 44, 43, 43, 43, 1,
	45, 1, 46, 45, 1, 47, 9, 1,
	0, 0, 0, 0, 0, 0, 0, 0,
	48, 1, 49, 49, 49, 1, 50, 50,
	50, 1, 51, 51, 51, 1, 0, 0,
	0, 1, 52, 53, 54, 55, 56, 57,
	59, 62, 63, 61, 64, 65, 52, 58,
	60, 61, 60, 61, 1, 66, 67, 68,
	69, 70, 71, 72, 73, 66, 1, 66,
	68, 69, 70, 71, 72, 73, 66, 1,
	74, 75, 76, 77, 78, 79, 81, 84,
	85, 83, 86, 87, 74, 80, 82, 83,
	82, 83, 1, 88, 89, 90, 91, 92,
	93, 94, 88, 1, 95, 96, 97, 98,
	99, 100, 102, 105, 106, 104, 107, 108,
	95, 101, 103, 104, 103, 104, 1, 109,
	110, 111, 112, 113, 114, 115, 109, 14,
	1, 116, 117, 118, 119, 120, 121, 122,
	116, 13, 1, 123, 124, 125, 126, 127,
	21, 128, 129, 21, 130, 131, 123, 15,
	1, 123, 125, 127, 128, 129, 130, 131,
	123, 1, 132, 133, 134, 135, 136, 137,
	138, 139, 132, 23, 1, 132, 134, 135,
	136, 137, 138, 139, 132, 1, 123, 124,
	125, 34, 126, 127, 141, 128, 129, 141,
	130, 131, 123, 140, 35, 35, 1, 142,
	143, 144, 145, 146, 147, 148, 149, 150,
	142, 28, 28, 28, 1, 142, 144, 146,
	147, 148, 149, 150, 142, 1, 142, 143,
	144, 146, 147, 148, 149, 150, 142, 33,
	33, 33, 1, 151, 152, 153, 145, 154,
	155, 156, 157, 158, 151, 38, 28, 28,
	1, 151, 153, 154, 155, 156, 157, 158,
	151, 1, 132, 133, 134, 34, 135, 136,
	137, 138, 139, 132, 37, 35, 35, 1,
	159, 160, 161, 34, 163, 165, 166, 164,
	167, 168, 159, 162, 162, 164, 162, 164,
	1, 159, 161, 163, 165, 166, 167, 168,
	159, 1, 159, 160, 161, 163, 165, 166,
	164, 167, 168, 159, 164, 164, 164, 1,
	169, 170, 171, 172, 173, 174, 175, 169,
	1,
}

var __RDX_trans_targs []byte = []byte{
	1, 0, 48, 42, 3, 4, 5, 49,
	7, 41, 8, 51, 10, 54, 53, 55,
	13, 14, 15, 56, 17, 18, 19, 57,
	21, 22, 23, 58, 60, 26, 27, 28,
	61, 62, 24, 30, 32, 65, 63, 34,
	35, 36, 64, 38, 39, 40, 67, 69,
	43, 44, 45, 46, 47, 1, 6, 9,
	47, 11, 59, 47, 66, 68, 47, 50,
	47, 52, 47, 2, 47, 47, 47, 50,
	47, 52, 47, 1, 6, 9, 47, 11,
	59, 47, 66, 68, 47, 50, 47, 52,
	47, 47, 47, 47, 50, 47, 52, 47,
	1, 6, 9, 47, 11, 59, 47, 66,
	68, 47, 50, 47, 52, 47, 47, 47,
	47, 50, 47, 52, 47, 47, 47, 47,
	50, 47, 52, 47, 12, 47, 16, 47,
	47, 50, 47, 52, 47, 20, 47, 47,
	47, 50, 47, 52, 59, 31, 47, 25,
	47, 29, 47, 47, 50, 47, 52, 47,
	33, 47, 47, 47, 50, 47, 52, 47,
	37, 47, 66, 47, 68, 47, 50, 47,
	52, 47, 47, 47, 47, 50, 47, 52,
}

var __RDX_trans_actions []byte = []byte{
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 1, 1, 1,
	29, 1, 1, 31, 1, 1, 25, 0,
	21, 0, 9, 0, 78, 81, 75, 9,
	72, 9, 27, 159, 159, 159, 168, 159,
	159, 171, 159, 159, 165, 27, 162, 27,
	17, 126, 129, 123, 17, 120, 17, 23,
	144, 144, 144, 153, 144, 144, 156, 144,
	144, 150, 23, 147, 23, 19, 138, 141,
	135, 19, 132, 19, 15, 114, 117, 111,
	15, 108, 15, 5, 0, 54, 0, 57,
	51, 5, 48, 5, 3, 0, 42, 45,
	39, 3, 36, 3, 0, 0, 7, 0,
	66, 0, 69, 63, 7, 60, 7, 33,
	0, 182, 186, 178, 33, 174, 33, 11,
	0, 90, 0, 93, 0, 87, 11, 84,
	11, 13, 102, 105, 99, 13, 96, 13,
}

var __RDX_eof_actions []byte = []byte{
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	9, 9, 27, 17, 23, 19, 15, 5,
	5, 3, 3, 5, 7, 7, 7, 33,
	33, 3, 11, 11, 11, 13,
}

const _RDX_start int = 47
const _RDX_first_final int = 47
const _RDX_error int = 0

const _RDX_en_main int = 47

//line rdx.rl:197

func ParseRDX(data []byte) (rdx *RDX, err error) {

	var mark [RdxMaxNesting]int
	nest, cs, p, pe, eof := 0, 0, 0, len(data), len(data)

	rdx = &RDX{}

//line rdx.ragel.go:296
	{
		cs = _RDX_start
	}

//line rdx.rl:207

//line rdx.ragel.go:301
	{
		var _klen int
		var _trans int
		var _acts int
		var _nacts uint
		var _keys int
		if p == pe {
			goto _test_eof
		}
		if cs == 0 {
			goto _out
		}
	_resume:
		_keys = int(__RDX_key_offsets[cs])
		_trans = int(__RDX_index_offsets[cs])

		_klen = int(__RDX_single_lengths[cs])
		if _klen > 0 {
			_lower := int(_keys)
			var _mid int
			_upper := int(_keys + _klen - 1)
			for {
				if _upper < _lower {
					break
				}

				_mid = _lower + ((_upper - _lower) >> 1)
				switch {
				case data[p] < __RDX_trans_keys[_mid]:
					_upper = _mid - 1
				case data[p] > __RDX_trans_keys[_mid]:
					_lower = _mid + 1
				default:
					_trans += int(_mid - int(_keys))
					goto _match
				}
			}
			_keys += _klen
			_trans += _klen
		}

		_klen = int(__RDX_range_lengths[cs])
		if _klen > 0 {
			_lower := int(_keys)
			var _mid int
			_upper := int(_keys + (_klen << 1) - 2)
			for {
				if _upper < _lower {
					break
				}

				_mid = _lower + (((_upper - _lower) >> 1) & ^1)
				switch {
				case data[p] < __RDX_trans_keys[_mid]:
					_upper = _mid - 2
				case data[p] > __RDX_trans_keys[_mid+1]:
					_lower = _mid + 2
				default:
					_trans += int((_mid - int(_keys)) >> 1)
					goto _match
				}
			}
			_trans += _klen
		}

	_match:
		_trans = int(__RDX_indicies[_trans])
		cs = int(__RDX_trans_targs[_trans])

		if __RDX_trans_actions[_trans] == 0 {
			goto _again
		}

		_acts = int(__RDX_trans_actions[_trans])
		_nacts = uint(__RDX_actions[_acts])
		_acts++
		for ; _nacts > 0; _nacts-- {
			_acts++
			switch __RDX_actions[_acts-1] {
			case 0:
//line rdx.rl:9
				mark[nest] = p
			case 1:
//line rdx.rl:10

				rdx.RdxType = Float
				rdx.Text = data[mark[nest]:p]

			case 2:
//line rdx.rl:14

				// I
				rdx.RdxType = Integer
				rdx.Text = data[mark[nest]:p]

			case 3:
//line rdx.rl:19

				// R
				if rdx.RdxType != Integer {
					rdx.RdxType = Reference
				}
				rdx.Text = data[mark[nest]:p]

			case 4:
//line rdx.rl:26

				// S
				rdx.RdxType = String
				rdx.Text = data[mark[nest]:p]

			case 5:
//line rdx.rl:31

				rdx.RdxType = Term
				rdx.Text = data[mark[nest]:p]

			case 6:
//line rdx.rl:35

				rdx.RdxType = Natural
				rdx.Text = data[mark[nest]:p]

			case 7:
//line rdx.rl:39

				rdx.RdxType = NInc
				rdx.Text = data[mark[nest]:p]

			case 8:
//line rdx.rl:43

				rdx.RdxType = ZCounter
				rdx.Text = data[mark[nest]:p]

			case 9:
//line rdx.rl:47

				rdx.RdxType = ZInc
				rdx.Text = data[mark[nest]:p]

			case 10:
//line rdx.rl:52

				// {
				n := rdx.Nested
				n = append(n, RDX{Parent: rdx})
				rdx.Nested = n
				rdx.RdxType = Mapping
				rdx = &n[len(n)-1]
				nest++

			case 11:
//line rdx.rl:61

				// }
				if rdx.Parent == nil {
					cs = _RDX_error
					p++
					goto _out

				}
				nest--
				rdx = rdx.Parent
				if rdx.RdxType != Eulerian && rdx.RdxType != Mapping {
					cs = _RDX_error
					p++
					goto _out

				}
				if len(rdx.Nested) == 1 {
					rdx.RdxType = Eulerian
				}
				if rdx.RdxType == Mapping {
					if (len(rdx.Nested) & 1) == 1 {
						cs = _RDX_error
						p++
						goto _out

					}
				}

			case 12:
//line rdx.rl:84

				// [
				n := rdx.Nested
				n = append(n, RDX{Parent: rdx})
				rdx.Nested = n
				rdx.RdxType = Linear
				rdx = &n[len(n)-1]
				nest++

			case 13:
//line rdx.rl:93

				// ]
				if rdx.Parent == nil {
					cs = _RDX_error
					p++
					goto _out

				}
				nest--
				rdx = rdx.Parent
				if rdx.RdxType != Linear {
					cs = _RDX_error
					p++
					goto _out

				}

			case 14:
//line rdx.rl:107

				// ,
				if rdx.Parent == nil {
					cs = _RDX_error
					p++
					goto _out

				}
				n := rdx.Parent.Nested
				if rdx.Parent.RdxType == Mapping {
					if len(n) == 1 {
						rdx.Parent.RdxType = Eulerian
					} else if (len(n) & 1) == 1 {
						cs = _RDX_error
						p++
						goto _out

					}
				}
				n = append(n, RDX{Parent: rdx.Parent})
				rdx.Parent.Nested = n
				rdx = &n[len(n)-1]

			case 15:
//line rdx.rl:127

				// :
				if rdx.Parent == nil {
					cs = _RDX_error
					p++
					goto _out

				}
				n := rdx.Parent.Nested
				if rdx.Parent.RdxType == Mapping {
					if (len(n) & 1) == 0 {
						cs = _RDX_error
						p++
						goto _out

					}
				} else {
					cs = _RDX_error
					p++
					goto _out

				}
				n = append(n, RDX{Parent: rdx.Parent})
				rdx.Parent.Nested = n
				rdx = &n[len(n)-1]

//line rdx.ragel.go:556
			}
		}

	_again:
		if cs == 0 {
			goto _out
		}
		p++
		if p != pe {
			goto _resume
		}
	_test_eof:
		{
		}
		if p == eof {
			__acts := __RDX_eof_actions[cs]
			__nacts := uint(__RDX_actions[__acts])
			__acts++
			for ; __nacts > 0; __nacts-- {
				__acts++
				switch __RDX_actions[__acts-1] {
				case 1:
//line rdx.rl:10

					rdx.RdxType = Float
					rdx.Text = data[mark[nest]:p]

				case 2:
//line rdx.rl:14

					// I
					rdx.RdxType = Integer
					rdx.Text = data[mark[nest]:p]

				case 3:
//line rdx.rl:19

					// R
					if rdx.RdxType != Integer {
						rdx.RdxType = Reference
					}
					rdx.Text = data[mark[nest]:p]

				case 4:
//line rdx.rl:26

					// S
					rdx.RdxType = String
					rdx.Text = data[mark[nest]:p]

				case 5:
//line rdx.rl:31

					rdx.RdxType = Term
					rdx.Text = data[mark[nest]:p]

				case 6:
//line rdx.rl:35

					rdx.RdxType = Natural
					rdx.Text = data[mark[nest]:p]

				case 7:
//line rdx.rl:39

					rdx.RdxType = NInc
					rdx.Text = data[mark[nest]:p]

				case 8:
//line rdx.rl:43

					rdx.RdxType = ZCounter
					rdx.Text = data[mark[nest]:p]

				case 9:
//line rdx.rl:47

					rdx.RdxType = ZInc
					rdx.Text = data[mark[nest]:p]

				case 11:
//line rdx.rl:61

					// }
					if rdx.Parent == nil {
						cs = _RDX_error
						p++
						goto _out

					}
					nest--
					rdx = rdx.Parent
					if rdx.RdxType != Eulerian && rdx.RdxType != Mapping {
						cs = _RDX_error
						p++
						goto _out

					}
					if len(rdx.Nested) == 1 {
						rdx.RdxType = Eulerian
					}
					if rdx.RdxType == Mapping {
						if (len(rdx.Nested) & 1) == 1 {
							cs = _RDX_error
							p++
							goto _out

						}
					}

				case 13:
//line rdx.rl:93

					// ]
					if rdx.Parent == nil {
						cs = _RDX_error
						p++
						goto _out

					}
					nest--
					rdx = rdx.Parent
					if rdx.RdxType != Linear {
						cs = _RDX_error
						p++
						goto _out

					}

//line rdx.ragel.go:678
				}
			}
		}

	_out:
		{
		}
	}

//line rdx.rl:208

	if nest != 0 || cs < _RDX_first_final {
		err = fmt.Errorf("RDX parsing failed at pos %d", p)
	}

	return
}
