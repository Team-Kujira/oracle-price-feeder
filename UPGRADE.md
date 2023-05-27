# v0.6.x

## Breaking Changes

### provider_min_override

This option has been replace by `provider_min_overrides` (plural). Please remove `provider_min_override` from your config, if set.

### provider_min_overrides

This option defines how many different sources need to provide a valid price in order to use it for price conversions and to be submitted to the chain. The default value for every asset is `3`. Please set it to `1` for every asset with only one available source:

```
[[provider_min_overrides]]
denoms = ["KUJI", "MNTA", "STATOM", "STOSMO", "USK", "WINK"]
providers = 1
```