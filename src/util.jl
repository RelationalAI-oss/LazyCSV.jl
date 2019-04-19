"""
At the beginning of a page that uses a Unicode character encoding you may find some bytes
that represent the Unicode code point U+FEFF BYTE ORDER MARK (abbreviated as BOM ).

This helper function just skips BOM if present.
"""
function consumeBOM!(io)
    # BOM character detection
    startpos = position(io)
    if !eof(io) && Parsers.peekbyte(io) == 0xef
        Parsers.readbyte(io)
        (!eof(io) && Parsers.readbyte(io) == 0xbb) || Parsers.fastseek!(io, startpos)
        (!eof(io) && Parsers.readbyte(io) == 0xbf) || Parsers.fastseek!(io, startpos)
    end
    return
end

const ASCII_SPACE = UInt8(' ')
const ASCII_TAB = UInt8('\t')
const ASCII_RETURN = UInt8('\r')
const ASCII_NEWLINE = UInt8('\n')

"""
    ws = *(
            %x20 /              ; Space
            %x09 /              ; Horizontal tab
            %x0A /              ; Line feed or New line
            %x0D )              ; Carriage return
"""
@inline iswhitespace(c) = c == ASCII_SPACE  ||
                          c == ASCII_TAB ||
                          c == ASCII_RETURN ||
                          c == ASCII_NEWLINE
"""
`resize!` will be called rarely and it's better not to be inlined to avoid code explosion
"""
@noinline resize_vec!(vec::AbstractVector{T}, new_size) where {T} = resize!(vec, new_size)
