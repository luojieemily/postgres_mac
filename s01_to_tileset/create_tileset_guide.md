https://docs.mapbox.com/help/tutorials/get-started-mts-and-tilesets-cli/

# 1. export MAPBOX_ACCESS_TOKEN=

不一定每次运行都需要，具体 token 见 config file

# 2. 如要删除原有 source,

tilesets delete-source luojieemily line_220919
tilesets delete-source luojieemily polygon_220919

# 3. upload source

tilesets upload-source luojieemily line_220919 /Users/luojie/MyTemp/line_220919.geojson
tilesets upload-source luojieemily polygon_220919 /Users/luojie/MyTemp/polygon_220919.geojson

# 4. delete 原有 tileset

tilesets delete luojieemily.one_tile_220919

# 5. create tileset

tilesets create luojieemily.one_tile_220919 --recipe /Users/luojie/MyCode/nextjs_data/to_tileset/one_tile_recipe.json --name "one_tile"

# 6. Publish your new tileset

tilesets publish luojieemily.one_tile_220919

# 7. check the status

tilesets status luojieemily.one_tile_220919
