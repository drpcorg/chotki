package rdx 

import "fmt";

%%{

machine RDX;

action b { mark[nest] = p; }
action f {
    rdx.RdxType = Float; 
    rdx.Text = data[mark[nest] : p];
}
action eint {     
    // I
    rdx.RdxType = Integer; 
    rdx.Text = data[mark[nest] : p];
}
action eref {     
    // R
    if rdx.RdxType != Integer {
        rdx.RdxType = Reference; 
    }
    rdx.Text = data[mark[nest] : p];
}
action estring {
    // S
    rdx.RdxType = String; 
    rdx.Text = data[mark[nest] : p];
}
action term {
    rdx.RdxType = Term; 
    rdx.Text = data[mark[nest] : p];
}
action n {
    rdx.RdxType = NCounter;
    rdx.Text = data[mark[nest] : p];
}
action dn {
    rdx.RdxType = NInc;
    rdx.Text = data[mark[nest] : p];
}
action z {
    rdx.RdxType = ZCounter;
    rdx.Text = data[mark[nest] : p];
}
action dz {
    rdx.RdxType = ZInc;
    rdx.Text = data[mark[nest] : p];
}

action opush {
    // {
    n := rdx.Nested 
    n = append(n, RDX{Parent: rdx})
    rdx.Nested = n
    rdx.RdxType = Map;
    rdx = &n[len(n)-1]
    nest++; 
}
action opop {
    // }
    if rdx.Parent==nil {
        cs = _RDX_error;
        fbreak;
    }
    nest--;
    rdx = rdx.Parent;
    if rdx.RdxType != ESet && rdx.RdxType!=Map {
        cs = _RDX_error;
        fbreak;
    }
    if len(rdx.Nested)==1 {
        rdx.RdxType = ESet
    }
    if rdx.RdxType == Map {
        if (len(rdx.Nested)&1)==1 {
            cs = _RDX_error;
            fbreak;
        }
    }
}

action apush { 
    // [
    n := rdx.Nested 
    n = append(n, RDX{Parent: rdx})
    rdx.Nested = n
    rdx.RdxType = LArray;
    rdx = &n[len(n)-1]
    nest++; 
}
action apop {
    // ]
    if rdx.Parent==nil {
        cs = _RDX_error;
        fbreak;
    }
    nest--;
    rdx = rdx.Parent;
    if rdx.RdxType != LArray {
        cs = _RDX_error;
        fbreak;
    }
}

action comma {
    // ,
    if rdx.Parent==nil {
        cs = _RDX_error;
        fbreak;
    }
    n := rdx.Parent.Nested 
    if rdx.Parent.RdxType==Map {
        if len(n)==1 {
            rdx.Parent.RdxType = ESet
        } else if (len(n)&1)==1 {
            cs = _RDX_error;
            fbreak;
        }
    }
    n = append(n, RDX{Parent: rdx.Parent})
    rdx.Parent.Nested = n
    rdx = &n[len(n)-1]
}

action colon {
    // :
    if rdx.Parent==nil {
        cs = _RDX_error;
        fbreak;
    }
    n := rdx.Parent.Nested 
    if rdx.Parent.RdxType==Map { 
        if (len(n)&1)==0 {
            cs = _RDX_error;
            fbreak;
        }
    } else {
        cs = _RDX_error;
        fbreak;
    }
    n = append(n, RDX{Parent: rdx.Parent})
    rdx.Parent.Nested = n
    rdx = &n[len(n)-1]
}

hex = [0-9a-fA-F];
dec = [0-9];
uni = "\\u" hex hex hex hex;
esc = "\\" ["\/\\bfnrt];
char = [^0x00..0x19"\\] | uni | esc;
asci = [_0-9a-zA-Z];

INT = ( "-"? dec+ ) >b %eint;
FLOAT = ( "-"? dec+ ("." dec+)? ([eE] [\-+]? dec+) ) >b %f;
STRING = ( ["] char* ["] ) >b %estring; 
REF = ( hex+ "-" hex+ ( "-" hex+ )? ) >b %eref;
TERM = ( [_a-zA-Z] asci* ) >b %term;
FIRST = INT | FLOAT | STRING | REF | TERM;


NCOUNT = ( "(" dec+ ")" ) >b %n;
NINC = ( "+" dec+ ) >b %dn;
ZCOUNT = ( "(" [\-+] dec+ ")" ) >b %z;
ZINC = ( ("++"|"--") dec+ ) >b %dz;
COUNT = NCOUNT | NINC | ZCOUNT | ZINC;

OOPEN = "{" @opush;
OCLOSE = "}" %opop;

AOPEN = "[" @apush;
ACLOSE = "]" %apop;

COMMA = "," @comma;
COLON = ":" @colon;

PUNCT = OOPEN | OCLOSE | AOPEN | ACLOSE | COMMA | COLON;

sep = PUNCT | space;
token = FIRST | COUNT;

RDX = token? ( sep+ token )*  sep*;

}%%

%%{

machine _RDX;
write data;
include RDX;

main := RDX;

}%%

func ParseRDX(data []byte) (rdx *RDX, err error) {

    var mark [RdxMaxNesting]int
    nest, cs, p, pe, eof := 0, 0, 0, len(data), len(data)

    rdx = &RDX{}

%%write init;
%%write exec;

    if nest != 0 || cs < _RDX_first_final {
        err = fmt.Errorf("RDX parsing failed at pos %d", p)
    }

    return
}
