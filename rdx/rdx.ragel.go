package rdx

import "fmt"

var __RDX_actions = []int8{0, 1, 0, 1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 8, 1, 9, 1, 10, 1, 11, 1, 12, 1, 13, 1, 14, 1, 15, 2, 1, 3, 2, 1, 10, 2, 1, 12, 2, 1, 14, 2, 1, 15, 2, 2, 7, 2, 2, 10, 2, 2, 12, 2, 2, 14, 2, 2, 15, 2, 3, 10, 2, 3, 12, 2, 3, 14, 2, 3, 15, 2, 4, 10, 2, 4, 12, 2, 4, 14, 2, 4, 15, 2, 5, 10, 2, 5, 12, 2, 5, 14, 2, 5, 15, 2, 6, 10, 2, 6, 12, 2, 6, 14, 2, 6, 15, 2, 8, 10, 2, 8, 12, 2, 8, 14, 2, 8, 15, 2, 9, 10, 2, 9, 12, 2, 9, 14, 2, 9, 15, 2, 11, 0, 2, 11, 10, 2, 11, 12, 2, 11, 14, 2, 11, 15, 2, 13, 0, 2, 13, 10, 2, 13, 12, 2, 13, 14, 2, 13, 15, 3, 1, 3, 10, 3, 1, 3, 12, 3, 1, 3, 14, 3, 1, 3, 15, 3, 2, 7, 10, 3, 2, 7, 12, 3, 2, 7, 14, 3, 2, 7, 15, 0}
var __RDX_key_offsets = []int16{0, 0, 24, 31, 35, 37, 40, 43, 45, 46, 52, 58, 60, 64, 68, 70, 77, 85, 91, 103, 110, 113, 122, 128, 134, 140, 146, 170, 194, 203, 227, 236, 260, 271, 295, 314, 330, 345, 356, 372, 388, 409, 425, 439, 0}
var __RDX_trans_keys = []byte{32, 34, 40, 43, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 34, 46, 57, 92, 120, 48, 49, 43, 45, 48, 57, 48, 57, 41, 48, 57, 43, 48, 57, 48, 57, 45, 48, 57, 65, 70, 97, 102, 48, 57, 65, 70, 97, 102, 48, 57, 69, 101, 48, 57, 43, 45, 48, 57, 48, 57, 45, 48, 57, 65, 70, 97, 102, 43, 45, 48, 57, 65, 70, 97, 102, 48, 57, 65, 70, 97, 102, 45, 95, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 95, 48, 57, 65, 90, 97, 122, 41, 48, 57, 34, 47, 92, 98, 102, 110, 114, 116, 117, 48, 57, 65, 70, 97, 102, 48, 57, 65, 70, 97, 102, 48, 57, 65, 70, 97, 102, 48, 57, 65, 70, 97, 102, 32, 34, 40, 43, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 32, 34, 40, 43, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 32, 44, 58, 91, 93, 123, 125, 9, 13, 32, 34, 40, 43, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 32, 44, 58, 91, 93, 123, 125, 9, 13, 32, 34, 40, 43, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 32, 44, 58, 91, 93, 123, 125, 9, 13, 48, 57, 32, 34, 40, 43, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 32, 44, 45, 46, 58, 69, 91, 93, 101, 123, 125, 9, 13, 48, 57, 65, 70, 97, 102, 32, 44, 45, 58, 91, 93, 123, 125, 9, 13, 48, 57, 65, 70, 97, 102, 32, 44, 58, 91, 93, 123, 125, 9, 13, 48, 57, 65, 70, 97, 102, 32, 44, 58, 91, 93, 123, 125, 9, 13, 48, 57, 32, 44, 45, 58, 91, 93, 123, 125, 9, 13, 48, 57, 65, 70, 97, 102, 32, 44, 45, 58, 91, 93, 123, 125, 9, 13, 48, 57, 65, 70, 97, 102, 32, 44, 45, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 70, 71, 90, 97, 102, 103, 122, 32, 44, 58, 91, 93, 95, 123, 125, 9, 13, 48, 57, 65, 90, 97, 122, 32, 44, 46, 58, 69, 91, 93, 101, 123, 125, 9, 13, 48, 57, 32, 44, 58, 91, 93, 123, 125, 9, 13, 0}
var __RDX_single_lengths = []int8{0, 12, 5, 2, 0, 1, 1, 0, 1, 0, 0, 0, 2, 2, 0, 1, 2, 0, 2, 1, 1, 9, 0, 0, 0, 0, 12, 12, 7, 12, 7, 12, 7, 12, 11, 8, 7, 7, 8, 8, 9, 8, 10, 7, 0}
var __RDX_range_lengths = []int8{0, 6, 1, 1, 1, 1, 1, 1, 0, 3, 3, 1, 1, 1, 1, 3, 3, 3, 5, 3, 1, 0, 3, 3, 3, 3, 6, 6, 1, 6, 1, 6, 2, 6, 4, 4, 4, 2, 4, 4, 6, 4, 2, 1, 0}
var __RDX_index_offsets = []int16{0, 0, 19, 26, 30, 32, 35, 38, 40, 42, 46, 50, 52, 56, 60, 62, 67, 73, 77, 85, 90, 93, 103, 107, 111, 115, 119, 138, 157, 166, 185, 194, 213, 223, 242, 258, 271, 283, 293, 306, 319, 335, 348, 361, 0}
var __RDX_cond_targs = []int8{1, 2, 3, 6, 1, 8, 1, 1, 26, 19, 1, 27, 1, 34, 18, 19, 18, 19, 0, 28, 0, 0, 21, 0, 0, 2, 4, 4, 20, 0, 5, 0, 30, 5, 0, 7, 42, 0, 32, 0, 7, 0, 35, 35, 35, 0, 36, 36, 36, 0, 12, 0, 13, 13, 12, 0, 14, 14, 37, 0, 37, 0, 9, 15, 15, 15, 0, 14, 17, 39, 15, 15, 0, 38, 35, 35, 0, 9, 41, 40, 40, 41, 40, 41, 0, 41, 41, 41, 41, 0, 43, 20, 0, 2, 2, 2, 2, 2, 2, 2, 2, 22, 0, 23, 23, 23, 0, 24, 24, 24, 0, 25, 25, 25, 0, 2, 2, 2, 0, 1, 2, 3, 6, 1, 8, 1, 1, 26, 19, 1, 27, 1, 34, 18, 19, 18, 19, 0, 1, 2, 3, 6, 1, 8, 1, 1, 26, 19, 1, 27, 1, 34, 18, 19, 18, 19, 0, 29, 29, 29, 29, 31, 29, 33, 29, 0, 29, 2, 3, 6, 29, 8, 29, 29, 31, 19, 29, 33, 29, 34, 18, 19, 18, 19, 0, 29, 29, 29, 29, 31, 29, 33, 29, 0, 29, 2, 3, 6, 29, 8, 29, 29, 31, 19, 29, 33, 29, 34, 18, 19, 18, 19, 0, 29, 29, 29, 29, 31, 29, 33, 29, 32, 0, 29, 2, 3, 6, 29, 8, 29, 29, 31, 19, 29, 33, 29, 34, 18, 19, 18, 19, 0, 29, 29, 9, 11, 29, 16, 29, 31, 16, 29, 33, 29, 34, 15, 15, 0, 29, 29, 10, 29, 29, 31, 29, 33, 29, 35, 35, 35, 0, 29, 29, 29, 29, 31, 29, 33, 29, 36, 36, 36, 0, 29, 29, 29, 29, 31, 29, 33, 29, 37, 0, 29, 29, 10, 29, 29, 31, 29, 33, 29, 38, 35, 35, 0, 29, 29, 9, 29, 29, 31, 29, 33, 29, 39, 15, 15, 0, 29, 29, 9, 29, 29, 31, 41, 29, 33, 29, 40, 40, 41, 40, 41, 0, 29, 29, 29, 29, 31, 41, 29, 33, 29, 41, 41, 41, 0, 29, 29, 11, 29, 13, 29, 31, 13, 29, 33, 29, 42, 0, 29, 29, 29, 29, 31, 29, 33, 29, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 0}
var __RDX_cond_actions = []int16{0, 1, 1, 1, 27, 1, 29, 23, 0, 1, 19, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 148, 148, 148, 157, 148, 160, 154, 25, 148, 151, 25, 25, 148, 148, 148, 148, 148, 0, 21, 133, 133, 133, 142, 133, 145, 139, 21, 133, 136, 21, 21, 133, 133, 133, 133, 133, 0, 9, 79, 82, 76, 9, 73, 9, 9, 0, 0, 1, 1, 1, 27, 1, 29, 23, 0, 1, 19, 0, 0, 1, 1, 1, 1, 1, 0, 15, 115, 118, 112, 15, 109, 15, 15, 0, 25, 148, 148, 148, 157, 148, 160, 154, 25, 148, 151, 25, 25, 148, 148, 148, 148, 148, 0, 17, 127, 130, 124, 17, 121, 17, 17, 0, 0, 21, 133, 133, 133, 142, 133, 145, 139, 21, 133, 136, 21, 21, 133, 133, 133, 133, 133, 0, 5, 55, 0, 0, 58, 0, 52, 5, 0, 49, 5, 5, 0, 0, 0, 0, 7, 67, 0, 70, 64, 7, 61, 7, 7, 0, 0, 0, 0, 7, 67, 70, 64, 7, 61, 7, 7, 0, 0, 0, 0, 3, 40, 43, 37, 3, 34, 3, 3, 0, 0, 31, 171, 0, 175, 167, 31, 163, 31, 31, 0, 0, 0, 0, 3, 40, 0, 43, 37, 3, 34, 3, 3, 0, 0, 0, 0, 11, 91, 0, 94, 88, 11, 0, 85, 11, 11, 0, 0, 0, 0, 0, 0, 11, 91, 94, 88, 11, 0, 85, 11, 11, 0, 0, 0, 0, 46, 187, 0, 191, 0, 183, 46, 0, 179, 46, 46, 0, 0, 13, 103, 106, 100, 13, 97, 13, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 15, 25, 17, 21, 5, 7, 7, 3, 31, 3, 11, 11, 46, 13, 0}
var __RDX_eof_trans = []int16{371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 0}
var _RDX_start int = 1
var _ = _RDX_start
var _RDX_first_final int = 28
var _ = _RDX_first_final
var _RDX_error int = 0
var _ = _RDX_error
var _RDX_en_main int = 1
var _ = _RDX_en_main

func ParseRDX(data []byte) (rdx *RDX, err error) {

	var mark [RdxMaxNesting]int
	nest, cs, p, pe, eof := 0, 0, 0, len(data), len(data)

	rdx = &RDX{}

	{
		cs = int(_RDX_start)

	}
	{
		var _klen int
		var _trans uint = 0
		var _keys int
		var _acts int
		var _nacts uint
	_resume:
		{

		}
		if p == pe && p != eof {
			goto _out

		}
		if p == eof {
			if __RDX_eof_trans[cs] > 0 {
				_trans = uint(__RDX_eof_trans[cs]) - 1

			}

		} else {
			_keys = int(__RDX_key_offsets[cs])

			_trans = uint(__RDX_index_offsets[cs])
			_klen = int(__RDX_single_lengths[cs])
			if _klen > 0 {
				var _lower int = _keys
				var _upper int = _keys + _klen - 1
				var _mid int
				for {
					if _upper < _lower {
						_keys += _klen
						_trans += uint(_klen)
						break

					}
					_mid = _lower + ((_upper - _lower) >> 1)
					if (data[p]) < __RDX_trans_keys[_mid] {
						_upper = _mid - 1

					} else if (data[p]) > __RDX_trans_keys[_mid] {
						_lower = _mid + 1

					} else {
						_trans += uint((_mid - _keys))
						goto _match

					}

				}

			}
			_klen = int(__RDX_range_lengths[cs])
			if _klen > 0 {
				var _lower int = _keys
				var _upper int = _keys + (_klen << 1) - 2
				var _mid int
				for {
					if _upper < _lower {
						_trans += uint(_klen)
						break

					}
					_mid = _lower + (((_upper - _lower) >> 1) & ^1)
					if (data[p]) < __RDX_trans_keys[_mid] {
						_upper = _mid - 2

					} else if (data[p]) > __RDX_trans_keys[_mid+1] {
						_lower = _mid + 2

					} else {
						_trans += uint(((_mid - _keys) >> 1))
						break

					}

				}

			}
		_match:
			{

			}

		}
		cs = int(__RDX_cond_targs[_trans])
		if __RDX_cond_actions[_trans] != 0 {
			_acts = int(__RDX_cond_actions[_trans])

			_nacts = uint(__RDX_actions[_acts])
			_acts += 1
			for _nacts > 0 {
				switch __RDX_actions[_acts] {
				case 0:
					{
						mark[nest] = p
					}

				case 1:
					{
						rdx.RdxType = Float
						rdx.Text = data[mark[nest]:p]
					}

				case 2:
					{
						rdx.RdxType = Integer
						rdx.Text = data[mark[nest]:p]
					}

				case 3:
					{
						if rdx.RdxType != Integer {
							rdx.RdxType = Reference
						}
						rdx.Text = data[mark[nest]:p]
					}

				case 4:
					{
						rdx.RdxType = String
						rdx.Text = data[mark[nest]:p]
					}

				case 5:
					{
						rdx.RdxType = RdxName
						rdx.Text = data[mark[nest]:p]
					}

				case 6:
					{
						rdx.RdxType = NCounter
						rdx.Text = data[mark[nest]:p]
					}

				case 7:
					{
						rdx.RdxType = NInc
						rdx.Text = data[mark[nest]:p]
					}

				case 8:
					{
						rdx.RdxType = ZCounter
						rdx.Text = data[mark[nest]:p]
					}

				case 9:
					{
						rdx.RdxType = ZInc
						rdx.Text = data[mark[nest]:p]
					}

				case 10:
					{
						n := rdx.Nested
						n = append(n, RDX{Parent: rdx})
						rdx.Nested = n
						rdx.RdxType = Map
						rdx = &n[len(n)-1]
						nest++
					}

				case 11:
					{
						if rdx.Parent == nil {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						nest--
						rdx = rdx.Parent
						if rdx.RdxType != ESet && rdx.RdxType != Map && rdx.RdxType != RdxObject {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						if len(rdx.Nested) == 1 {
							rdx.RdxType = ESet
						}
						if rdx.RdxType == Map {
							if (len(rdx.Nested) & 1) == 1 {
								cs = _RDX_error
								{
									p += 1
									goto _out

								}

							}
						}
					}

				case 12:
					{
						n := rdx.Nested
						n = append(n, RDX{Parent: rdx})
						rdx.Nested = n
						rdx.RdxType = LArray
						rdx = &n[len(n)-1]
						nest++
					}

				case 13:
					{
						if rdx.Parent == nil {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						nest--
						rdx = rdx.Parent
						if rdx.RdxType != LArray {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
					}

				case 14:
					{
						if rdx.Parent == nil {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						n := rdx.Parent.Nested
						if rdx.Parent.RdxType == Map {
							if len(n) == 1 {
								rdx.Parent.RdxType = ESet
							} else if (len(n) & 1) == 1 {
								cs = _RDX_error
								{
									p += 1
									goto _out

								}

							}
						}
						n = append(n, RDX{Parent: rdx.Parent})
						rdx.Parent.Nested = n
						rdx = &n[len(n)-1]
					}

				case 15:
					{
						if rdx.Parent == nil {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						n := rdx.Parent.Nested
						if rdx.Parent.RdxType == Map {
							if (len(n) & 1) == 0 {
								cs = _RDX_error
								{
									p += 1
									goto _out

								}

							}
						} else if rdx.Parent.RdxType == RdxObject {
							if (len(n) & 1) == 0 {
								cs = _RDX_error
								{
									p += 1
									goto _out

								}

							}
							if rdx.RdxType != RdxName {
								cs = _RDX_error
								{
									p += 1
									goto _out

								}

							}
						} else {
							cs = _RDX_error
							{
								p += 1
								goto _out

							}

						}
						n = append(n, RDX{Parent: rdx.Parent})
						rdx.Parent.Nested = n
						rdx = &n[len(n)-1]
					}

				}
				_nacts -= 1
				_acts += 1

			}

		}
		if p == eof {
			if cs >= 28 {
				goto _out

			}

		} else {
			if cs != 0 {
				p += 1
				goto _resume

			}

		}
	_out:
		{

		}

	}
	if nest != 0 || cs < _RDX_first_final {
		err = fmt.Errorf("RDX parsing failed at pos %d", p)
	}

	return
}
