package rdx 

import "fmt";

%%{

machine RDX;

action b { mark[nest] = p; }
action eint {     
    // I
    rdx.RdxType = RdxInt; 
    rdx.Text = data[mark[nest] : p];
}
action eref {     
    // R
    if rdx.RdxType != RdxInt {
        rdx.RdxType = RdxRef; 
    }
    rdx.Text = data[mark[nest] : p];
}
action estring {
    // S
    rdx.RdxType = RdxString; 
    rdx.Text = data[mark[nest] : p];
}
action name {
    rdx.RdxType = RdxName; 
    rdx.Text = data[mark[nest] : p];
}

action opush {
    // {
    n := rdx.Nested 
    n = append(n, RDX{Parent: rdx})
    rdx.Nested = n
    rdx.RdxType = RdxMap;
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
    if rdx.RdxType != RdxSet && rdx.RdxType!=RdxMap && rdx.RdxType!=RdxObject {
        cs = _RDX_error;
        fbreak;
    }
    if len(rdx.Nested)==1 {
        rdx.RdxType = RdxSet
    }
}

action apush { 
    // [
    n := rdx.Nested 
    n = append(n, RDX{Parent: rdx})
    rdx.Nested = n
    rdx.RdxType = RdxArray;
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
    if rdx.RdxType != RdxArray {
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
    if rdx.Parent.RdxType==RdxMap {
        if len(n)==1 {
            rdx.Parent.RdxType = RdxSet
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
    if rdx.Parent.RdxType==RdxMap { 
        if (len(n)&1)==0 {
            cs = _RDX_error;
            fbreak;
        }
    } else if rdx.Parent.RdxType==RdxObject {
        if (len(n)&1)==0 {
            cs = _RDX_error;
            fbreak;
        }
        if rdx.RdxType != RdxName {
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
sign = [\-+];
dec = [0-9];
uni = "\\u" hex hex hex hex;
esc = "\\" ["\/\\bfnrt];
char = [^0x00..0x19"\\] | uni | esc;
asci = [_0-9a-zA-Z];

INT = ( sign? dec+ ) >b %eint;
FLOAT = sign? dec+ ("." dec+)? ([eE] sign? dec+);
STRING = ( ["] char* ["] ) >b %estring; 
REF = ( hex+ "-" hex+ ( "-" hex+ )? ) >b %eref;
NULL = "null";
FIRST = INT | FLOAT | STRING | REF | NULL;

NAME = ( [_a-zA-Z] asci+ ) >b %name;

OOPEN = "{" @opush;
OCLOSE = "}" %opop;

AOPEN = "[" @apush;
ACLOSE = "]" %apop;

COMMA = "," @comma;
COLON = ":" @colon;

PUNCT = OOPEN | OCLOSE | AOPEN | ACLOSE | COMMA | COLON;

sep = PUNCT | space;
token = FIRST | NAME;

RDX = sep* token ( sep+ token )*  sep*;

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

    if cs < _RDX_first_final {
        err = fmt.Errorf("RDX parsing failed at pos %d", p)
    }

    return
}
