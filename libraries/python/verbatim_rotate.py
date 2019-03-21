import libconf
import io

# The path to the verbatim log that will be compressed using logrotate depends on the coin used. The path can be found
# in the provided netmine.cfg, so we will parse it and replace it in verbatim-rotate.cfg

# Read verbatim-rotate
with open('/coinscope/tools/resources/verbatim-rotate.cfg') as vrotate:
  vrotate_data = vrotate.read()

# Read netmine
with io.open("/coinscope/netmine.cfg", encoding='utf-8') as netmine:
    cfg = libconf.load(netmine)
    path = cfg.get("verbatim").get("logpath")
    # Replace the place holder (absolute_path) with the actual path
    vrotate_data = vrotate_data.replace("absolute_path/", path)

# Write verbatim-rotate back
with open('/coinscope/tools/resources/verbatim-rotate.cfg', 'w') as vrotate:
    vrotate.write(vrotate_data)

