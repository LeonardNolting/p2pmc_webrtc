extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn cancellable(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    // Check if the function is async
    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(input_fn, "The cancellable macro can only be applied to async functions")
            .to_compile_error()
            .into();
    }

    // Clone the original function to generate the new one, removing the #[cancellable] attribute
    let mut original_fn = input_fn.clone();
    original_fn.attrs.retain(|attr| !attr.path().is_ident("cancellable"));

    // Extract necessary components from the original function
    let visibility = &original_fn.vis;
    let fn_name = &original_fn.sig.ident;
    let cancellable_name = syn::Ident::new(&format!("{}_cancellable", fn_name), fn_name.span());
    let generics = &original_fn.sig.generics;
    let params = &original_fn.sig.inputs;
    let where_clause = &original_fn.sig.generics.where_clause;

    // Determine the return type
    let _return_type = match &original_fn.sig.output {
        syn::ReturnType::Default => quote! { () },
        syn::ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Extract parameter names for passing to the original function
    let param_names: Vec<_> = original_fn.sig.inputs.iter()
        .map(|param| {
            match param {
                syn::FnArg::Receiver(_) => quote! { self },
                syn::FnArg::Typed(pat_type) => {
                    if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                        let ident = &pat_ident.ident;
                        quote! { #ident }
                    } else {
                        syn::Error::new_spanned(pat_type, "cancellable macro only supports simple identifiers in function parameters")
                            .to_compile_error()
                            .into()
                    }
                }
            }
        })
        .collect();

    // Generate the new function
    let generated = quote! {
        #original_fn

        /// flutter_rust_bridge:sync
        #visibility fn #cancellable_name #generics (#params) -> tokio_util::sync::CancellationToken #where_clause {
            let token = tokio_util::sync::CancellationToken::new();
            let cloned_token = token.clone();
            tokio::spawn(async move {
                // Wait for either cancellation or a very long time
                tokio::select! {
                    _ = cloned_token.cancelled() => {}
                    _ = async move {
                        self::#fn_name(#(#param_names),*).await
                    } => {}
                }
            });
            token
        }
    };

    TokenStream::from(generated)
}