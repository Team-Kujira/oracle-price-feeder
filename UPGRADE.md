# Upgrade

## v0.8.x

### Breaking Changes

#### go v1.20

Required go version is now `1.20`

#### osmosis

This provider has been removed (osmosisv2 is still available)

#### bkex

This provider has been removed

## v0.7.x

### Breaking Changes

#### osmosisv2

This provider has been reworked to support all available Osmosis pools. Therefore it is necessary to provide a map of the pairs and their corresponfing pool id in `config.toml`:

Example:

```toml
[contract_addresses.osmosisv2]
STATOMATOM = "1136"
STOSMOOSMO = "833"
MNTAOSMO = "1215"
```

## v0.6.x

### Breaking Changes

#### derivative

The derivative `tvwap` has been renamed to `twap`. Please replace all occurances of `derivative = "tvwap"` with `derivative = "twap"` in the config.

## v0.5.x

### Breaking Changes

#### provider_min_override

This option has been replaced by `provider_min_overrides` (plural). Please remove `provider_min_override` from your config, if set.

#### provider_min_overrides

This option defines how many different sources need to provide a valid price in order to use it for price conversions and to be submitted to the chain. The default value for every asset is `3`. Please set it to `1` for every asset with only one available source:

```toml
[[provider_min_overrides]]
denoms = ["KUJI", "MNTA", "STATOM", "STOSMO", "USK", "WINK"]
providers = 1
```
