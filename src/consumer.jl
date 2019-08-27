using Dates

const SHOW_DETAILED_ERROR = false

"""
`DataConsumer` is the abstract type for all the concrete structs that have an implementation
for digesting the data parsed by `LazyCSV`
"""
abstract type DataConsumer end

"""
    consume(consumer::DataConsumer, f::File)

This function is only called once per `LazyCSV.File` for a given `DataConsumer` instance.

The purpose of this functions is to iterate over the whole file.

First, the header is consumed (via `consume_header` based on consumer's knowledge of whether
       it exists of not) and the the corresponding header is produced inside the consumer if
       necessary (via `produce_header`).
Then, each line of the body is traversed and consumed separately (via `consume_rec`).
      And then inside each record, each field is consumed (via `consume_field`).
      If any error is found in processing the records or field, the corresponding error
      handling function (either `consume_rec_error` or `consume_field_error`
      correspondingly) is called.
Finally, after all records are processed, the consumption is finalized (via a call to
      `consume_finalize`)

If this sequence of operations is adequate for a consumer, it's suggested to overload the
    inner functions (i.e., consume_header, produce_header, consume_rec, consume_field,
                           consume_rec_error, consume_field_error, consume_finalize).
    Otherwise, a consumer can overload the `consume` method directly and change its behavior.
"""
function consume(consumer::DataConsumer, f::File)
    iter_res = iterate(f)
    (iter_res, new_consumer) = consume_header(consumer, f, iter_res)
    while iter_res !== nothing
        ((pos, line), state) = iter_res
        consume_rec(new_consumer, f, pos, line, first_rec_fields(f))
        iter_res = iterate(f, state)
    end
    consume_finalize(new_consumer)
end
function consume_header end
function produce_header end
function consume_rec end
function consume_field end
function consume_rec_error end
function consume_field_error end
function consume_finalize end

function consume_header(fn::Function, consumer::DataConsumer, file::File, iter_res)
    new_iter_res = if iter_res !== nothing && file.header_exists
        ((pos, line), state) = iter_res
        i = 1
        for field_header in first_rec_fields(file)
            fn(field_header, i)
            i += 1
        end
        iterate(file, state)
    else
        iter_res
    end
    updated_consumer = produce_header(consumer, file)
    (new_iter_res, updated_consumer)
end

"""
Creates a key object for each row for a given `FilePos` (which is the byte sequence number
of the first character in the line of that record in the CSV source and it's a unique ID for
each row)
"""
create_key(pc::DataConsumer, pos::FilePos) = pos

function consume_rec(pc::DataConsumer, f::File, pos::FilePos, line, fields)
    key = create_key(pc, pos)
    if !validate_record(pc.rec_type, length(fields))
        consume_rec_error(pc, f, key, line)
    else
        for (i, field) in enumerate(fields)
            if !consume_field(pc, f, key, line, field, i)
                consume_field_error(pc, f, key, line, field, i)
            end
        end
    end
end

"""
This function is applied on the consumer, when processing a CSV file is done
"""
function consume_finalize(consumer::DataConsumer) end

"""
An abstract type for specifying the record length.

Its concrete extensions are `UnknownRecordLength` and `FixedRecordLength`.

The only method that has to be overloaded for it is `Base.length`.
"""
abstract type RecordLength end
struct UnknownRecordLength <: RecordLength end
struct FixedRecordLength <: RecordLength
    len::Int
end
Base.length(::UnknownRecordLength) = -1
Base.length(rec_len::FixedRecordLength) = rec_len.len

# Currently, only the first byte in LSB is used for storing the type information
# The second byte is currently reserved (to be used by user libraries for adding
# a few more user defined types).
# The third and fourth bytes can be used for storing additional type information
# if required.
const FieldType = UInt64

# The list of all supported types in CSV data loading
const NO_TYPE       = 0b000000000 % UInt64
const MISSING_TYPE  = 0b100000000 % UInt64
const INT_TYPE      = 0b000000001 % UInt64
const FLOAT_TYPE    = 0b000000010 % UInt64
const DATE_TYPE     = 0b000000100 % UInt64
const DATETIME_TYPE = 0b000001000 % UInt64
const BOOL_TYPE     = 0b000010000 % UInt64
const CHAR_TYPE     = 0b000100000 % UInt64
const STRING_TYPE   = 0b001000000 % UInt64

typecode(::Type{Int64}) = INT_TYPE
typecode(::Type{Float64}) = FLOAT_TYPE
typecode(::Type{Date}) = DATE_TYPE
typecode(::Type{DateTime}) = DATETIME_TYPE
typecode(::Type{Bool}) = BOOL_TYPE
typecode(::Type{String}) = STRING_TYPE
typecode(::Type{Char}) = CHAR_TYPE
typecode(::Type{Tuple{Ptr{UInt8}, Int}}) = STRING_TYPE
typecode(::Type{Missing}) = MISSING_TYPE
typecode(::Type) = NO_TYPE

"""
An abstract type for specifying the type of a record in a CSV file.
    
Two methods have to be defined for each subtype of it:
    - record_length: length of the record, which should be of type `RecordLength`
    - record_fields: a vector of `FieldType`, one for each column
"""
abstract type RecordType end
function record_length end
function record_fields end

function validate_record(rec_type::RecordType, len::UInt)
    rec_len = record_length(rec_type)
    rec_len == FixedRecordLength(len) || rec_len == UnknownRecordLength()
end

struct UntypedRecord <: RecordType
    num_fields::Int
end
function record_length(rec_type::UntypedRecord)
    rec_type.num_fields <= 0 ? UnknownRecordLength() : FixedRecordLength(rec_type.num_fields)
end
function record_fields(rec_type::UntypedRecord)
    field_types = Vector{FieldType}()
    for i in 1:rec_type.num_fields
        push!(field_types, NO_TYPE)
    end
    field_types
end
function typecodes(tp::Type{Union{A,B}}) where {A,B}
    if tp isa DataType
        typecode(tp)
    elseif tp.b isa DataType
        typecode(tp.b) | typecode(tp.a)
    else
        typecodes(tp.b) | typecode(tp.a)
    end
end

function get_field_union_types(::Type{T}) where {T <: Tuple}
    res = Vector{FieldType}()
    for field_tp in T.parameters
        field_types = typecodes(field_tp)
        push!(res, field_types)
    end
    res
end

struct FieldHandler{HANDLER <: Function}
    fn::HANDLER
end

struct TypedRecord{FIELD_HANDLERS <: Tuple} <: RecordType
    field_types::Vector{FieldType}
    fallback_type::FieldType
    other_type_handlers::FIELD_HANDLERS
    function TypedRecord(field_types::Vector{FieldType}=Vector{FieldType}(),
                         fallback_type::FieldType=NO_TYPE,
                         other_type_handlers::FIELD_HANDLERS=Tuple{}()) where {FIELD_HANDLERS}
        new{FIELD_HANDLERS}(field_types, fallback_type, other_type_handlers)
    end
end

function TypedRecord(field_types::Type{T},
                     fallback_type::FieldType=NO_TYPE,
                     other_type_handlers::FIELD_HANDLERS=Tuple{}()) where {FIELD_HANDLERS, T <: Tuple}
    TypedRecord(get_field_union_types(field_types), fallback_type, other_type_handlers)
end

function record_length(rec_type::TypedRecord)
    length(rec_type.field_types) == 0 ? UnknownRecordLength() : FixedRecordLength(length(rec_type.field_types))
end
function record_fields(rec_type::TypedRecord)
    rec_type.field_types
end

function field_type(rec_type::TypedRecord, i::Int)
    if i <= length(rec_type.field_types)
        rec_type.field_types[i]
    elseif i <= length(rec_type.other_type_handlers)
        NO_TYPE
    else
        rec_type.fallback_type
    end
end

ismissing_value(field_value::AbstractString) = isempty(field_value)

function has_valid_type(tp::FieldType, field_value::AbstractString)
    tp == NO_TYPE && return (false, NO_TYPE)
    tp & MISSING_TYPE  != 0 && ismissing_value(field_value) && return (true, MISSING_TYPE)
        if tp & INT_TYPE      != 0 && tryparse(Int, field_value) !== nothing; return (true, INT_TYPE)
    elseif tp & FLOAT_TYPE    != 0 && tryparse(Float64, field_value) !== nothing; return (true, FLOAT_TYPE)
    elseif tp & DATE_TYPE     != 0 && tryparse(Date, field_value) !== nothing; return (true, DATE_TYPE)
    elseif tp & DATETIME_TYPE != 0 && tryparse(DateTime, field_value) !== nothing; return (true, DATETIME_TYPE)
    elseif tp & BOOL_TYPE     != 0 && tryparse(Bool, field_value) !== nothing; return (true, BOOL_TYPE)
    elseif tp & CHAR_TYPE     != 0 && length(field_value) == 1; return (true, CHAR_TYPE)
    elseif tp & STRING_TYPE   != 0; return (true, STRING_TYPE)
    end
    return (false, NO_TYPE)
end

include("print_consumer.jl")

export DataConsumer, consume, consume_header, consume_rec, consume_field, consume_rec_error,
       consume_field_error
export RecordLength, UnknownRecordLength, FixedRecordLength
export FieldType, RecordType, UntypedRecord, record_length, csv_string, record_fields
export MISSING_TYPE, INT_TYPE, FLOAT_TYPE, DATE_TYPE, DATETIME_TYPE
export BOOL_TYPE, CHAR_TYPE, STRING_TYPE
