project('distributed-system', 'cpp', version : '0.1')

# For common include dirs, compile flags, dependencies
cpp = meson.get_compiler('cpp')
zmq = dependency('libzmq')

# Expose dependencies and include dirs to subdirectories
# common_inc = include_directories('common')

# subdir('common')
subdir('client')
# subdir('gateway')
# subdir('broker')
