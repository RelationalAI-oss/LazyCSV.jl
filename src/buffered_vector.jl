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
    setindex!(vec.buff, val, key)
    vec.size += 1
    nothing
end

Base.getindex(vec::BufferedVector{T}, key) where {T} = getindex(vec.buff, key)
Base.length(vec::BufferedVector{T}) where {T} = vec.size
Base.empty!(vec::BufferedVector{T}) where {T} = (vec.size = 0; nothing)
