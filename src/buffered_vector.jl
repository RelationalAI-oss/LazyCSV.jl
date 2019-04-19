"""
`BufferedVector` is a wrapper around `Vector` that avoid shrinking the inner `Vector`

This is done by keeping a `size` in itself that represets the number of usable elements in
the unerlying `Vector`.

Note: if an element is added to this `Vector`, it might not get garbage collected, as a
      pointer to it might remain in the underlying `Vector`. Thus, storing huge objects in
      `BufferedVector` is not encouraged.
"""
mutable struct BufferedVector{T} <: AbstractVector{T}
    buff::Vector{T}
    size::IntTp
    
    BufferedVector{T}() where {T} = new{T}(Vector{T}(), 0)
    BufferedVector(buff::Vector{T}) where {T} = new{T}(buff, 0)
end
function Base.iterate(vec::BufferedVector{T}, state::Int=1) where {T}
    state > vec.size ? nothing : (vec.buff[state], state+1)
end
Base.resize!(vec::BufferedVector{T}, new_size::Int) where {T} = resize!(vec.buff, new_size)
function Base.setindex!(vec::BufferedVector{T}, val, key) where {T}
    # `setindex!` is supposed to be called in a sequence
    @assert key == vec.size + 1
    push!(vec.buff, val)
    vec.size += 1
    nothing
end

Base.getindex(vec::BufferedVector{T}, key) where {T} = getindex(vec.buff, key)
Base.length(vec::BufferedVector{T}) where {T} = vec.size
Base.empty!(vec::BufferedVector{T}) where {T} = (vec.size = 0; nothing)

Base.IndexStyle(::Type{<:BufferedVector}) = IndexLinear()
Base.IndexStyle(::BufferedVector) = IndexLinear()
Base.size(vec::BufferedVector) = (vec.size,)
Base.eltype(vec::BufferedVector{T}) where {T} = T
Base.eltype(::Type{BufferedVector{T}}) where {T} = T
